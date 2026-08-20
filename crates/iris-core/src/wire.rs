//! Public wire decoding for generated surfaces.
//!
//! The `send_message` operation accepts an optional `attachments` field on
//! HTTP and MCP bodies (a closed inline/stored union, see
//! `openspec/changes/add-outbound-attachments`) and repeatable `--attach` /
//! `--attach-mime` CLI flags. This module owns the pure decoding half of
//! that contract: JSON union values become
//! [`OutboundAttachment`](crate::OutboundAttachment) variants, and raw CLI
//! flag values become a validated plan that the CLI boundary turns into
//! inline bytes by reading local files. All rejection happens here, before
//! any provider dispatch.

use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::{IrisError, OutboundAttachment, Result};

/// Prefix for stored attachment references on the CLI.
const STORED_REF_PREFIX: &str = "iris://attachment/";

/// MIME type used when a local attachment's extension is not recognized.
pub const FALLBACK_MIME_TYPE: &str = "application/octet-stream";

/// Decode the optional `attachments` body field of `send_message`.
///
/// `None` and JSON `null` decode to an empty list. Any other value must be
/// an array whose items are exactly one of the closed union variants:
///
/// - inline: required `mime_type` (non-empty) and `data_base64` (valid,
///   non-empty decode), optional `filename`, nothing else
/// - stored: exactly one key `stored_id` holding a valid UUID
///
/// Items that mix variants, carry unknown fields, or fail these rules are
/// rejected with a descriptive [`IrisError::Config`].
pub fn decode_attachments(value: Option<&serde_json::Value>) -> Result<Vec<OutboundAttachment>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    let Some(items) = value.as_array() else {
        return Err(invalid("attachments must be an array"));
    };
    items.iter().map(decode_item).collect()
}

fn decode_item(item: &serde_json::Value) -> Result<OutboundAttachment> {
    let Some(object) = item.as_object() else {
        return Err(invalid("each attachment must be an object"));
    };
    let has_inline = object.contains_key("mime_type") || object.contains_key("data_base64");
    let has_stored = object.contains_key("stored_id");
    if has_inline && has_stored {
        return Err(invalid(
            "attachment mixes inline and stored fields; use exactly one variant",
        ));
    }
    if has_stored {
        if object.len() != 1 {
            return Err(invalid("stored attachment must carry only stored_id"));
        }
        let raw = object
            .get("stored_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| invalid("stored attachment stored_id must be a UUID string"))?;
        let id = Uuid::parse_str(raw)
            .map_err(|_| invalid(format!("stored attachment has an invalid UUID: {raw}")))?;
        return Ok(OutboundAttachment::Stored(id));
    }
    if !has_inline {
        return Err(invalid(
            "attachment must be inline (mime_type + data_base64) or stored (stored_id)",
        ));
    }
    for key in object.keys() {
        if !matches!(key.as_str(), "mime_type" | "filename" | "data_base64") {
            return Err(invalid(format!(
                "inline attachment carries an unknown field: {key}"
            )));
        }
    }
    let mime_type = object
        .get("mime_type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid("inline attachment requires a mime_type string"))?;
    if mime_type.trim().is_empty() {
        return Err(invalid("inline attachment requires a non-empty mime_type"));
    }
    let data_base64 = object
        .get("data_base64")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| invalid("inline attachment requires a data_base64 string"))?;
    let bytes = base64_decode(data_base64)?;
    if bytes.is_empty() {
        return Err(invalid("inline attachment requires non-empty bytes"));
    }
    let filename = match object.get("filename") {
        None => None,
        // The declared schema makes `filename` optional but not nullable:
        // an explicit null is outside the closed union and must be rejected.
        Some(serde_json::Value::Null) => {
            return Err(invalid(
                "inline attachment filename must be a string when present (null is not allowed)",
            ));
        }
        Some(value) => Some(
            value
                .as_str()
                .ok_or_else(|| invalid("inline attachment filename must be a string"))?
                .to_owned(),
        ),
    };
    Ok(OutboundAttachment::Bytes {
        mime_type: mime_type.to_owned(),
        filename,
        bytes,
    })
}

fn base64_decode(data: &str) -> Result<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|_| invalid("inline attachment data_base64 is not valid base64"))
}

/// One planned `--attach` value, before any file is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannedAttachment {
    /// A local file to read at the CLI boundary, with the MIME type to send
    /// (explicit `--attach-mime` override or extension inference) and the
    /// filename to attribute (the file's own name).
    LocalFile {
        /// Filesystem path to read.
        path: PathBuf,
        /// MIME type resolved for this attachment.
        mime_type: String,
        /// Filename to attribute, if any.
        filename: Option<String>,
    },
    /// A stored `iris://attachment/{uuid}` reference.
    Stored(Uuid),
}

/// Plan repeatable `--attach` / `--attach-mime` CLI values.
///
/// Each raw `--attach` value is either a local path or an
/// `iris://attachment/{uuid}` reference. When `attach_mime` is non-empty it
/// must contain exactly one value per local-path attachment, applied in
/// local-path order (stored references consume none); each value overrides
/// MIME inference for its attachment. When empty, MIME types are inferred
/// from file extensions with [`FALLBACK_MIME_TYPE`] as the default.
pub fn plan_attachments(
    attach: &[String],
    attach_mime: &[String],
) -> Result<Vec<PlannedAttachment>> {
    let local_count = attach
        .iter()
        .filter(|value| !value.starts_with("iris://"))
        .count();
    if !attach_mime.is_empty() && attach_mime.len() != local_count {
        return Err(invalid(format!(
            "--attach-mime supplied {} value(s) for {} local-path attachment(s); \
             counts must match exactly and apply in order",
            attach_mime.len(),
            local_count,
        )));
    }
    let mut next_mime = attach_mime.iter();
    let mut planned = Vec::with_capacity(attach.len());
    for value in attach {
        if let Some(raw) = value.strip_prefix(STORED_REF_PREFIX) {
            let id = Uuid::parse_str(raw)
                .map_err(|_| invalid(format!("invalid stored attachment reference: {value}")))?;
            planned.push(PlannedAttachment::Stored(id));
            continue;
        }
        if value.starts_with("iris://") {
            return Err(invalid(format!(
                "unsupported iris:// reference (expected {STORED_REF_PREFIX}UUID): {value}"
            )));
        }
        let path = PathBuf::from(value);
        let mime_type = next_mime
            .next()
            .map_or_else(|| infer_mime_type(&path), Clone::clone);
        if mime_type.trim().is_empty() {
            return Err(invalid("--attach-mime values must be non-empty MIME types"));
        }
        let filename = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned());
        planned.push(PlannedAttachment::LocalFile {
            path,
            mime_type,
            filename,
        });
    }
    Ok(planned)
}

/// Infer a MIME type from a file extension, defaulting to
/// [`FALLBACK_MIME_TYPE`].
#[must_use]
pub fn infer_mime_type(path: &Path) -> String {
    let Some(extension) = path
        .extension()
        .map(|extension| extension.to_string_lossy().to_ascii_lowercase())
    else {
        return FALLBACK_MIME_TYPE.to_owned();
    };
    let mime = match extension.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "svg" => "image/svg+xml",
        "txt" => "text/plain",
        "md" => "text/markdown",
        "csv" => "text/csv",
        "html" | "htm" => "text/html",
        "json" => "application/json",
        "xml" => "application/xml",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "gz" => "application/gzip",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "m4a" => "audio/mp4",
        "flac" => "audio/flac",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mkv" => "video/x-matroska",
        "mov" => "video/quicktime",
        _ => return FALLBACK_MIME_TYPE.to_owned(),
    };
    mime.to_owned()
}

fn invalid(message: impl Into<String>) -> IrisError {
    IrisError::Config(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    fn decode(value: &serde_json::Value) -> Result<Vec<OutboundAttachment>> {
        decode_attachments(Some(value))
    }

    fn inline(data_base64: &str) -> serde_json::Value {
        json!({
            "mime_type": "image/png",
            "filename": "chart.png",
            "data_base64": data_base64,
        })
    }

    #[test]
    fn absent_and_null_attachments_decode_to_empty() {
        assert!(decode_attachments(None).unwrap().is_empty());
        assert!(decode(&json!(null)).unwrap().is_empty());
        assert!(decode(&json!([])).unwrap().is_empty());
    }

    #[test]
    fn inline_attachment_decodes_with_optional_filename() {
        let attachments = decode(&json!([inline("aGk=")])).unwrap();
        assert_eq!(
            attachments,
            vec![OutboundAttachment::Bytes {
                mime_type: "image/png".to_owned(),
                filename: Some("chart.png".to_owned()),
                bytes: b"hi".to_vec(),
            }]
        );
        let bare = json!({"mime_type": "text/plain", "data_base64": "aGk="});
        let attachments = decode(&json!([bare])).unwrap();
        assert_eq!(
            attachments,
            vec![OutboundAttachment::Bytes {
                mime_type: "text/plain".to_owned(),
                filename: None,
                bytes: b"hi".to_vec(),
            }]
        );
    }

    #[test]
    fn stored_attachment_decodes_uuid() {
        let id = Uuid::new_v4();
        let attachments = decode(&json!([{ "stored_id": id.to_string() }])).unwrap();
        assert_eq!(attachments, vec![OutboundAttachment::Stored(id)]);
    }

    #[test]
    fn order_is_preserved_across_variants() {
        let id = Uuid::new_v4();
        let attachments =
            decode(&json!([inline("aGk="), { "stored_id": id.to_string() }])).unwrap();
        assert_eq!(attachments.len(), 2);
        assert!(matches!(attachments[0], OutboundAttachment::Bytes { .. }));
        assert_eq!(attachments[1], OutboundAttachment::Stored(id));
    }

    #[test]
    fn non_array_attachments_are_rejected() {
        assert!(decode(&json!("nope")).is_err());
        assert!(decode(&json!({ "mime_type": "a" })).is_err());
    }

    #[test]
    fn mixed_variants_are_rejected() {
        let mixed = json!({
            "mime_type": "image/png",
            "data_base64": "aGk=",
            "stored_id": Uuid::new_v4().to_string(),
        });
        let error = decode(&json!([mixed])).unwrap_err();
        assert!(error.to_string().contains("mixes"), "{error}");
    }

    #[test]
    fn unknown_fields_are_rejected_on_both_variants() {
        let unknown_inline = json!({
            "mime_type": "image/png",
            "data_base64": "aGk=",
            "caption": "nope",
        });
        assert!(decode(&json!([unknown_inline])).is_err());
        let unknown_stored = json!({ "stored_id": Uuid::new_v4().to_string(), "filename": "nope" });
        let error = decode(&json!([unknown_stored])).unwrap_err();
        assert!(error.to_string().contains("only stored_id"), "{error}");
    }

    #[test]
    fn inline_requires_mime_type_and_data_base64() {
        assert!(decode(&json!([{ "mime_type": "image/png" }])).is_err());
        assert!(decode(&json!([{ "data_base64": "aGk=" }])).is_err());
        assert!(
            decode(&json!([{
                "mime_type": "   ",
                "data_base64": "aGk=",
            }]))
            .is_err()
        );
        assert!(
            decode(&json!([{
                "mime_type": "image/png",
                "data_base64": 7,
            }]))
            .is_err()
        );
    }

    #[test]
    fn malformed_base64_and_empty_bytes_are_rejected() {
        let error = decode(&json!([inline("!!!not-base64!!!")])).unwrap_err();
        assert!(error.to_string().contains("base64"), "{error}");
        let error = decode(&json!([inline("")])).unwrap_err();
        assert!(error.to_string().contains("non-empty"), "{error}");
    }

    #[test]
    fn invalid_stored_uuid_is_rejected() {
        let error = decode(&json!([{ "stored_id": "not-a-uuid" }])).unwrap_err();
        assert!(error.to_string().contains("invalid UUID"), "{error}");
    }

    #[test]
    fn null_filename_is_rejected() {
        // `filename` is optional but not nullable in the declared union
        // schema; an explicit null is outside the closed contract.
        let mut item = inline("aGVsbG8=");
        item.as_object_mut()
            .expect("inline helper builds an object")
            .insert("filename".to_owned(), serde_json::Value::Null);
        let error = decode(&json!([item])).unwrap_err();
        assert!(error.to_string().contains("null is not allowed"), "{error}");
    }

    #[test]
    fn non_object_items_are_rejected() {
        assert!(decode(&json!(["path.png"])).is_err());
        assert!(decode(&json!([7])).is_err());
    }

    #[test]
    fn cli_blank_attach_mime_override_is_rejected() {
        // --attach-mime overrides must be non-empty MIME types, matching the
        // HTTP/MCP inline contract (which rejects blank mime_type values).
        let error =
            plan_attachments(&["/tmp/photo.png".to_owned()], &["   ".to_owned()]).unwrap_err();
        assert!(error.to_string().contains("non-empty MIME"), "{error}");
        let error = plan_attachments(&["/tmp/photo.png".to_owned()], &[String::new()]).unwrap_err();
        assert!(error.to_string().contains("non-empty MIME"), "{error}");
    }

    #[test]
    fn cli_plan_splits_local_paths_and_stored_refs() {
        let id = Uuid::new_v4();
        let plan = plan_attachments(
            &[
                "/tmp/photo.png".to_owned(),
                format!("iris://attachment/{id}"),
                "/tmp/notes.txt".to_owned(),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(
            plan,
            vec![
                PlannedAttachment::LocalFile {
                    path: PathBuf::from("/tmp/photo.png"),
                    mime_type: "image/png".to_owned(),
                    filename: Some("photo.png".to_owned()),
                },
                PlannedAttachment::Stored(id),
                PlannedAttachment::LocalFile {
                    path: PathBuf::from("/tmp/notes.txt"),
                    mime_type: "text/plain".to_owned(),
                    filename: Some("notes.txt".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn cli_mime_overrides_apply_in_local_path_order() {
        let plan = plan_attachments(
            &[
                "/tmp/a.dat".to_owned(),
                "iris://attachment/00000000-0000-0000-0000-000000000001".to_owned(),
                "/tmp/b.dat".to_owned(),
            ],
            &[
                "application/x-first".to_owned(),
                "application/x-second".to_owned(),
            ],
        )
        .unwrap();
        let mimes: Vec<_> = plan
            .iter()
            .filter_map(|item| match item {
                PlannedAttachment::LocalFile { mime_type, .. } => Some(mime_type.clone()),
                PlannedAttachment::Stored(_) => None,
            })
            .collect();
        assert_eq!(
            mimes,
            vec![
                "application/x-first".to_owned(),
                "application/x-second".to_owned(),
            ]
        );
    }

    #[test]
    fn cli_mime_count_must_match_local_paths_exactly() {
        let error = plan_attachments(
            &["/tmp/a.png".to_owned(), "/tmp/b.png".to_owned()],
            &["image/png".to_owned()],
        )
        .unwrap_err();
        assert!(error.to_string().contains("must match"), "{error}");
        let error = plan_attachments(
            &["iris://attachment/00000000-0000-0000-0000-000000000002".to_owned()],
            &["image/png".to_owned()],
        )
        .unwrap_err();
        assert!(error.to_string().contains("must match"), "{error}");
        assert_eq!(
            plan_attachments(
                &["iris://attachment/00000000-0000-0000-0000-000000000003".to_owned()],
                &[]
            )
            .unwrap()
            .len(),
            1
        );
    }

    #[test]
    fn cli_unknown_extensions_fall_back_to_octet_stream() {
        let mime = infer_mime_type(Path::new("/tmp/blob.what"));
        assert_eq!(mime, "application/octet-stream");
        let mime = infer_mime_type(Path::new("/tmp/noext"));
        assert_eq!(mime, "application/octet-stream");
        let mime = infer_mime_type(Path::new("/tmp/Photo.PNG"));
        assert_eq!(mime, "image/png");
    }

    #[test]
    fn cli_invalid_iris_refs_are_rejected() {
        let error =
            plan_attachments(&["iris://attachment/not-a-uuid".to_owned()], &[]).unwrap_err();
        assert!(error.to_string().contains("invalid stored"), "{error}");
        let error = plan_attachments(&["iris://other/thing".to_owned()], &[]).unwrap_err();
        assert!(error.to_string().contains("unsupported iris://"), "{error}");
    }
}
