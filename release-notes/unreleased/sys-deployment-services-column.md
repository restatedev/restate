# Release Notes: Add `services` column to `sys_deployment` table

## New Feature

### What Changed

The `sys_deployment` introspection table now includes a `services` column containing the list of service names registered by each deployment. This enables SQL queries to find deployments by service name.

### Why This Matters

Previously, finding which deployments registered a particular service required either:
- Using the REST API and filtering results client-side
- Using the CLI's `restate deployments list --extra` which performs in-memory joins

With this change, users can now query deployments by service name directly via SQL, enabling use cases like:
- Finding outdated/drained deployments that originally registered a specific service
- Identifying all deployments associated with a service across its revision history
- Building custom tooling around deployment lifecycle management

### Example Queries

```sql
-- List all deployments with their services
SELECT id, endpoint, created_at, services FROM sys_deployment;

-- Find all deployments that registered a specific service
SELECT id, endpoint, created_at, services
FROM sys_deployment
WHERE array_has(services, 'MyService');

-- Find outdated deployments for a service (all except the most recent)
WITH ranked AS (
  SELECT id, created_at, services,
    ROW_NUMBER() OVER (ORDER BY created_at DESC) as rn
  FROM sys_deployment
  WHERE array_has(services, 'MyService')
)
SELECT id FROM ranked WHERE rn > 1;

-- List all services and their deployment IDs
SELECT unnest(services) as service, id as deployment_id
FROM sys_deployment;
```

### Impact on Users

- **New deployments**: The `services` column is automatically populated
- **Existing deployments**: Will show their services after upgrading
- **No breaking changes**: This is an additive schema change

### Related Changes

- Added `LargeUtf8List` type support to the table macro infrastructure for future use
