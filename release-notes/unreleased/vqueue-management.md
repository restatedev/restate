# Manage virtual queues

## New Feature

Virtual queues can now be paused and resumed through the Admin API using
`POST /vqueues/{vqueue_id}/pause` and `POST /vqueues/{vqueue_id}/resume`.

The Restate CLI adds `restate vqueues` commands to:

- List virtual queues and their entry counts with `restate vqueues list`.
- Inspect a virtual queue and its entries with `restate vqueues describe <VQUEUE_ID>`.
- Pause or resume processing with `restate vqueues pause <VQUEUE_ID>` and
  `restate vqueues resume <VQUEUE_ID>`.

Pause and resume requests are applied asynchronously. No configuration or migration changes are
required.
