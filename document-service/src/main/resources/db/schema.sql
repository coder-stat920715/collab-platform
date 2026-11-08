-- PostgreSQL schema for the production persistence layer (the demo service keeps data in memory).
CREATE TABLE workspace (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, name text NOT NULL, created_at timestamptz DEFAULT now());
CREATE TABLE document (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, workspace_id uuid REFERENCES workspace(id),
  title text NOT NULL, head_rev bigint DEFAULT 0, created_by uuid, created_at timestamptz DEFAULT now());
CREATE TABLE document_acl (
  document_id uuid REFERENCES document(id), principal_id uuid,
  role text CHECK (role IN ('VIEWER','EDITOR','ADMIN')), PRIMARY KEY (document_id, principal_id));
CREATE TABLE doc_revision (
  document_id uuid, rev bigint, s3_key text NOT NULL, author_ids uuid[], created_at timestamptz DEFAULT now(),
  PRIMARY KEY (document_id, rev));
CREATE TABLE comment_thread (
  id uuid PRIMARY KEY, document_id uuid, tenant_id uuid, anchor_start int, anchor_end int,
  anchor_rev bigint, resolved boolean DEFAULT false);
CREATE TABLE comment (
  id uuid PRIMARY KEY, thread_id uuid REFERENCES comment_thread(id), author_id uuid, body text,
  created_at timestamptz DEFAULT now());

ALTER TABLE document ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON document USING (tenant_id = current_setting('app.tenant_id')::uuid);
-- repeat ENABLE ROW LEVEL SECURITY + policy for every tenant-scoped table
