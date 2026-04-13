const dbName = "wuliu_p1_mongodb_db";
const p1Db = db.getSiblingDB(dbName);

function ensureCollection(name, validator) {
  const exists = p1Db.getCollectionNames().includes(name);
  if (!exists) {
    p1Db.createCollection(name, { validator });
  } else {
    p1Db.runCommand({ collMod: name, validator });
  }
}

function dropAllNonIdIndexes(name) {
  const coll = p1Db.getCollection(name);
  coll.getIndexes().forEach((idx) => {
    if (idx.name !== "_id_") {
      try {
        coll.dropIndex(idx.name);
      } catch (e) {
        // ignore
      }
    }
  });
}

ensureCollection("event_log", {
  $jsonSchema: {
    bsonType: "object",
    required: ["event_id", "waybill_no", "event_type", "event_time"],
    properties: {
      event_id: { bsonType: "string" },
      waybill_no: { bsonType: "string" },
      event_type: { bsonType: "string" },
      event_time: { bsonType: ["date", "string"] },
      operator_id: { bsonType: "string" },
      node_id: { bsonType: "string" },
      status_before: { bsonType: "string" },
      status_after: { bsonType: "string" },
      payload: { bsonType: ["object", "array", "string"] }
    }
  }
});

ensureCollection("trace_document", {
  $jsonSchema: {
    bsonType: "object",
    required: ["trace_id", "master_waybill_no", "current_status"],
    properties: {
      trace_id: { bsonType: "string" },
      master_waybill_no: { bsonType: "string" },
      package_list: { bsonType: ["array", "string"] },
      node_path: { bsonType: ["array", "string"] },
      time_path: { bsonType: ["array", "string"] },
      current_status: { bsonType: "string" },
      last_node: { bsonType: "string" },
    }
  }
});

ensureCollection("exception_report", {
  $jsonSchema: {
    bsonType: "object",
    required: ["report_id", "waybill_no", "exception_type", "report_time"],
    properties: {
      report_id: { bsonType: "string" },
      waybill_no: { bsonType: "string" },
      exception_type: { bsonType: "string" },
      description: { bsonType: "string" },
      attachments: { bsonType: ["array", "string"] },
      reporter: { bsonType: "string" },
      report_time: { bsonType: ["date", "string"] },
      process_logs: { bsonType: ["array", "object", "string"] }
    }
  }
});

ensureCollection("api_payload_archive", {
  $jsonSchema: {
    bsonType: "object",
    required: ["archive_id", "source_system", "target_system", "api_name", "biz_id", "call_time"],
    properties: {
      archive_id: { bsonType: "string" },
      source_system: { bsonType: "string" },
      target_system: { bsonType: "string" },
      api_name: { bsonType: "string" },
      biz_type: { bsonType: "string" },
      biz_id: { bsonType: "string" },
      request_body: { bsonType: ["object", "string"] },
      response_body: { bsonType: ["object", "string"] },
      call_time: { bsonType: ["date", "string"] },
      status_code: { bsonType: ["int", "string"] },
      trace_id: { bsonType: "string" },
      retry_flag: { bsonType: "string" },
      error_message: { bsonType: "string" },
    }
  }
});

ensureCollection("receipt_image_meta", {
  $jsonSchema: {
    bsonType: "object",
    required: ["image_id", "waybill_no", "file_name", "file_url", "upload_time"],
    properties: {
      image_id: { bsonType: "string" },
      waybill_no: { bsonType: "string" },
      package_no: { bsonType: "string" },
      file_name: { bsonType: "string" },
      file_url: { bsonType: "string" },
      file_type: { bsonType: "string" },
      biz_scene: { bsonType: "string" },
      upload_time: { bsonType: ["date", "string"] },
      uploader: { bsonType: "string" },
      source_system: { bsonType: "string" },
      checksum_md5: { bsonType: "string" },
      remark: { bsonType: "string" }
    }
  }
});

[
  "event_log",
  "trace_document",
  "exception_report",
  "api_payload_archive",
  "receipt_image_meta"
].forEach(dropAllNonIdIndexes);

p1Db.event_log.createIndex({ event_id: 1 }, { unique: true, name: "uk_event_log_event_id" });
p1Db.event_log.createIndex({ waybill_no: 1 }, { name: "idx_event_log_waybill_no" });
p1Db.event_log.createIndex({ event_time: -1 }, { name: "idx_event_log_event_time" });
p1Db.event_log.createIndex({ event_type: 1 }, { name: "idx_event_log_event_type" });
p1Db.event_log.createIndex({ waybill_no: 1, event_time: -1 }, { name: "idx_event_log_waybill_time" });

p1Db.trace_document.createIndex({ trace_id: 1 }, { unique: true, name: "uk_trace_document_trace_id" });
p1Db.trace_document.createIndex({ master_waybill_no: 1 }, { unique: true, name: "uk_trace_document_master_waybill_no" });
p1Db.trace_document.createIndex({ current_status: 1 }, { name: "idx_trace_document_current_status" });
p1Db.trace_document.createIndex({ last_node: 1 }, { name: "idx_trace_document_last_node" });

p1Db.exception_report.createIndex({ report_id: 1 }, { unique: true, name: "uk_exception_report_report_id" });
p1Db.exception_report.createIndex({ waybill_no: 1 }, { name: "idx_exception_report_waybill_no" });
p1Db.exception_report.createIndex({ exception_type: 1 }, { name: "idx_exception_report_exception_type" });
p1Db.exception_report.createIndex({ report_time: -1 }, { name: "idx_exception_report_report_time" });

p1Db.api_payload_archive.createIndex({ archive_id: 1 }, { unique: true, name: "uk_api_payload_archive_archive_id" });
p1Db.api_payload_archive.createIndex({ source_system: 1 }, { name: "idx_api_payload_archive_source_system" });
p1Db.api_payload_archive.createIndex({ target_system: 1 }, { name: "idx_api_payload_archive_target_system" });
p1Db.api_payload_archive.createIndex({ biz_id: 1 }, { name: "idx_api_payload_archive_biz_id" });
p1Db.api_payload_archive.createIndex({ call_time: -1 }, { name: "idx_api_payload_archive_call_time" });

p1Db.receipt_image_meta.createIndex({ image_id: 1 }, { unique: true, name: "uk_receipt_image_meta_image_id" });
p1Db.receipt_image_meta.createIndex({ waybill_no: 1 }, { name: "idx_receipt_image_meta_waybill_no" });
p1Db.receipt_image_meta.createIndex({ package_no: 1 }, { name: "idx_receipt_image_meta_package_no" });
p1Db.receipt_image_meta.createIndex({ upload_time: -1 }, { name: "idx_receipt_image_meta_upload_time" });
