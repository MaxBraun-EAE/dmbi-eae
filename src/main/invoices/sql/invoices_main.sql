SELECT
  invoice_id
, inv.document_type_id
, invoice_date
, total_import_euros
, contract_id
, energy_active_kwh
, client_id
, invoice_start_date
, invoice_end_date
, doctype.document_type_description
FROM eae.inv_invoice_ft inv
left join eae.inv_document_type_dim doctype
on inv.document_type_id=doctype.document_type_id
where invoice_date>='2024-01-01'