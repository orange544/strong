const dbName = "wuliu_d1_mongodb_db";
const d1Db = db.getSiblingDB(dbName);

function ensureCollection(name, validator) {
  const exists = d1Db.getCollectionNames().includes(name);
  if (!exists) {
    d1Db.createCollection(name, { validator });
  } else {
    d1Db.runCommand({ collMod: name, validator });
  }
}

function dropAllNonIdIndexes(name) {
  const coll = d1Db.getCollection(name);
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

ensureCollection("delivery_task", {
  $jsonSchema: {
    bsonType: "object",
    required: ["task_id", "task_no", "waybill_no", "package_no", "courier_id", "station_id", "delivery_status", "dispatch_time"],
    properties: {
      task_id: { bsonType: "string" },
      task_no: { bsonType: "string" },
      waybill_no: { bsonType: "string" },
      package_no: { bsonType: "string" },
      courier_id: { bsonType: "string" },
      station_id: { bsonType: "string" },
      delivery_status: { bsonType: "string" },
      dispatch_time: { bsonType: ["date", "string"] },
      planned_sign_time: { bsonType: ["date", "string", "null"] },
      actual_sign_time: { bsonType: ["date", "string", "null"] },
      recipient_name: { bsonType: "string" },
      recipient_phone: { bsonType: "string" },
      delivery_address: { bsonType: "string" },
      remark: { bsonType: "string" }
    }
  }
});

ensureCollection("sign_record", {
  $jsonSchema: {
    bsonType: "object",
    required: ["sign_id", "task_id", "waybill_no", "sign_type", "sign_time"],
    properties: {
      sign_id: { bsonType: "string" },
      task_id: { bsonType: "string" },
      waybill_no: { bsonType: "string" },
      sign_type: { bsonType: "string" },
      sign_time: { bsonType: ["date", "string"] },
      signer_name: { bsonType: "string" },
      signer_relation: { bsonType: "string" },
      proof_images: { bsonType: ["array", "string"] },
      geo_lng: { bsonType: ["double", "decimal", "int", "long"] },
      geo_lat: { bsonType: ["double", "decimal", "int", "long"] },
      remark: { bsonType: "string" }
    }
  }
});

ensureCollection("complaint_ticket", {
  $jsonSchema: {
    bsonType: "object",
    required: ["ticket_id", "waybill_no", "complaint_type", "process_status"],
    properties: {
      ticket_id: { bsonType: "string" },
      waybill_no: { bsonType: "string" },
      complaint_type: { bsonType: "string" },
      description: { bsonType: "string" },
      process_status: { bsonType: "string" },
      closed_time: { bsonType: ["date", "string", "null"] }
    }
  }
});

ensureCollection("customer_feedback", {
  $jsonSchema: {
    bsonType: "object",
    required: ["feedback_id", "task_id", "feedback_type", "feedback_time"],
    properties: {
      feedback_id: { bsonType: "string" },
      task_id: { bsonType: "string" },
      feedback_type: { bsonType: "string" },
      feedback_content: { bsonType: "string" },
      feedback_time: { bsonType: ["date", "string"] },
      customer_score: { bsonType: ["double", "decimal", "int", "long"] },
      channel_type: { bsonType: "string" },
    }
  }
});

ensureCollection("proof_of_delivery_meta", {
  $jsonSchema: {
    bsonType: "object",
    required: ["pod_id", "task_id", "waybill_no", "file_url", "upload_time", "verify_status"],
    properties: {
      pod_id: { bsonType: "string" },
      task_id: { bsonType: "string" },
      waybill_no: { bsonType: "string" },
      file_url: { bsonType: "string" },
      file_type: { bsonType: "string" },
      upload_user: { bsonType: "string" },
      upload_time: { bsonType: ["date", "string"] },
      verify_status: { bsonType: "string" }
    }
  }
});

ensureCollection("address_parse_doc", {
  $jsonSchema: {
    bsonType: "object",
    required: ["addr_doc_id", "raw_address", "std_address", "station_id", "parse_status"],
    properties: {
      addr_doc_id: { bsonType: "string" },
      raw_address: { bsonType: "string" },
      std_address: { bsonType: "string" },
      province: { bsonType: "string" },
      city: { bsonType: "string" },
      district: { bsonType: "string" },
      geo_lng: { bsonType: ["double", "decimal", "int", "long"] },
      geo_lat: { bsonType: ["double", "decimal", "int", "long"] },
      station_id: { bsonType: "string" },
      parse_status: { bsonType: "string" },
    }
  }
});

[
  "delivery_task",
  "sign_record",
  "complaint_ticket",
  "customer_feedback",
  "proof_of_delivery_meta",
  "address_parse_doc"
].forEach(dropAllNonIdIndexes);

d1Db.delivery_task.createIndex({ task_id: 1 }, { unique: true, name: "uk_delivery_task_task_id" });
d1Db.delivery_task.createIndex({ task_no: 1 }, { unique: true, name: "uk_delivery_task_task_no" });
d1Db.delivery_task.createIndex({ waybill_no: 1 }, { name: "idx_delivery_task_waybill_no" });
d1Db.delivery_task.createIndex({ package_no: 1 }, { name: "idx_delivery_task_package_no" });
d1Db.delivery_task.createIndex({ courier_id: 1 }, { name: "idx_delivery_task_courier_id" });
d1Db.delivery_task.createIndex({ station_id: 1 }, { name: "idx_delivery_task_station_id" });
d1Db.delivery_task.createIndex({ delivery_status: 1 }, { name: "idx_delivery_task_delivery_status" });
d1Db.delivery_task.createIndex({ dispatch_time: -1 }, { name: "idx_delivery_task_dispatch_time" });

d1Db.sign_record.createIndex({ sign_id: 1 }, { unique: true, name: "uk_sign_record_sign_id" });
d1Db.sign_record.createIndex({ task_id: 1 }, { name: "idx_sign_record_task_id" });
d1Db.sign_record.createIndex({ waybill_no: 1 }, { name: "idx_sign_record_waybill_no" });
d1Db.sign_record.createIndex({ sign_time: -1 }, { name: "idx_sign_record_sign_time" });
d1Db.sign_record.createIndex({ sign_type: 1 }, { name: "idx_sign_record_sign_type" });

d1Db.complaint_ticket.createIndex({ ticket_id: 1 }, { unique: true, name: "uk_complaint_ticket_ticket_id" });
d1Db.complaint_ticket.createIndex({ waybill_no: 1 }, { name: "idx_complaint_ticket_waybill_no" });
d1Db.complaint_ticket.createIndex({ complaint_type: 1 }, { name: "idx_complaint_ticket_complaint_type" });
d1Db.complaint_ticket.createIndex({ process_status: 1 }, { name: "idx_complaint_ticket_process_status" });
d1Db.customer_feedback.createIndex({ feedback_id: 1 }, { unique: true, name: "uk_customer_feedback_feedback_id" });
d1Db.customer_feedback.createIndex({ task_id: 1 }, { name: "idx_customer_feedback_task_id" });
d1Db.customer_feedback.createIndex({ feedback_type: 1 }, { name: "idx_customer_feedback_feedback_type" });
d1Db.customer_feedback.createIndex({ feedback_time: -1 }, { name: "idx_customer_feedback_feedback_time" });

d1Db.proof_of_delivery_meta.createIndex({ pod_id: 1 }, { unique: true, name: "uk_pod_meta_pod_id" });
d1Db.proof_of_delivery_meta.createIndex({ task_id: 1 }, { name: "idx_pod_meta_task_id" });
d1Db.proof_of_delivery_meta.createIndex({ waybill_no: 1 }, { name: "idx_pod_meta_waybill_no" });
d1Db.proof_of_delivery_meta.createIndex({ upload_time: -1 }, { name: "idx_pod_meta_upload_time" });
d1Db.proof_of_delivery_meta.createIndex({ verify_status: 1 }, { name: "idx_pod_meta_verify_status" });

d1Db.address_parse_doc.createIndex({ addr_doc_id: 1 }, { unique: true, name: "uk_address_parse_doc_addr_doc_id" });
d1Db.address_parse_doc.createIndex({ station_id: 1 }, { name: "idx_address_parse_doc_station_id" });
d1Db.address_parse_doc.createIndex({ parse_status: 1 }, { name: "idx_address_parse_doc_parse_status" });
d1Db.address_parse_doc.createIndex(
  { raw_address: "text", std_address: "text" },
  { name: "txt_address_parse_doc_raw_std" }
);
