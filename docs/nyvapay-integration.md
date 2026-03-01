## NyvaPay Card Payments Integration

This document explains how to wire NyvaPay into your app: configuration, API calls, webhooks, and the data model.

---

### 1. What NyvaPay Does

NyvaPay exposes a **Payment Links API**:

1. Your backend calls NyvaPay to create a **payment link**.
2. You redirect the user or show them the link to complete the payment.
3. NyvaPay calls your **webhook URL** on status changes.
4. Your backend uses the webhook to **credit the user exactly once** and finalize the invoice.

This pattern works with any framework or language (Node, Python, PHP, etc.) as long as you can:

- Make outbound HTTPS requests.
- Expose an HTTPS webhook endpoint.
- Persist some state in a database.

---

### 2. Requirements

- A NyvaPay merchant account with:
  - Merchant email
  - API key
- A public HTTPS URL for:
  - **Webhook endpoint**, e.g. `https://yourdomain.com/webhooks/nyvapay`
  - **Success redirect**, e.g. `https://yourdomain.com/billing/success`
- A place to store:
  - Invoices / billing ledger (e.g. `billing_history` table)
  - NyvaPay payment events (e.g. `nyvapay_payments` table)

---

### 3. Configuration

Add the following environment variables (or the equivalent in your config system):

- `NYVAPAY_API_URL` (optional; default `https://nyvapay.com/api`)
- `NYVAPAY_MERCHANT_EMAIL`
- `NYVAPAY_API_KEY`
- `NYVAPAY_WEBHOOK_URL` (public URL to your NyvaPay webhook in this project)
- `PUBLIC_BASE_URL` (base URL of your app, used for success redirects)

Example `.env` snippet:

```bash
NYVAPAY_API_URL=https://nyvapay.com/api
NYVAPAY_MERCHANT_EMAIL=merchant@example.com
NYVAPAY_API_KEY=YOUR_SECRET_KEY_HERE
NYVAPAY_WEBHOOK_URL=https://yourdomain.com/webhooks/nyvapay
PUBLIC_BASE_URL=https://yourdomain.com
```

---

### 4. HTTP Client Helper

In your backend, create a small NyvaPay client that:

- Points at `NYVAPAY_API_URL`
- Sets `X-Merchant-Email` and `X-API-Key` on every request

Example (Node.js + Axios):

```js
const axios = require('axios');

const NYVAPAY_API_URL = process.env.NYVAPAY_API_URL || 'https://nyvapay.com/api';
const NYVAPAY_MERCHANT_EMAIL = process.env.NYVAPAY_MERCHANT_EMAIL || '';
const NYVAPAY_API_KEY = process.env.NYVAPAY_API_KEY || '';

function nyvaPayClient() {
  if (!NYVAPAY_MERCHANT_EMAIL || !NYVAPAY_API_KEY) {
    throw new Error('NyvaPay is not configured (missing merchant email or API key)');
  }

  return axios.create({
    baseURL: NYVAPAY_API_URL,
    headers: {
      'X-Merchant-Email': NYVAPAY_MERCHANT_EMAIL,
      'X-API-Key': NYVAPAY_API_KEY,
      'Content-Type': 'application/json',
    },
    timeout: 15000,
  });
}
```

Adapt this pattern to your HTTP library / language if youre not using Node.

---

### 5. Data Model

The integration assumes two core concepts:

1. **Invoice / Billing row** in your own ledger (e.g. `billing_history`):
   - `id`, `user_id`, `amount`, `status`, `payment_method`, `reference_id`, `error`, timestamps.
   - One row per refill / invoice in *your* system.

2. **NyvaPay tracking row** in a dedicated table (e.g. `nyvapay_payments`):
   - `user_id`, `order_id`, `payment_link_id`, `nyvapay_payment_id`, `amount`, `currency`, `status`, `credited`, `raw_payload`, timestamps.
   - One row per NyvaPay payment link, used for idempotency and reporting.

Example SQL schema for `nyvapay_payments`:

```sql
CREATE TABLE IF NOT EXISTS nyvapay_payments (
  id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_id            BIGINT UNSIGNED NOT NULL,
  payment_link_id    VARCHAR(191) NULL,
  order_id           VARCHAR(191) NOT NULL,
  nyvapay_payment_id VARCHAR(191) NULL,
  amount             DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  currency           VARCHAR(16) NOT NULL DEFAULT 'USD',
  status             VARCHAR(64) NOT NULL DEFAULT 'pending',
  credited           TINYINT NOT NULL DEFAULT 0, -- 0=not, 1=credited, 2=processing
  raw_payload        JSON NULL,
  created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_user_id (user_id),
  UNIQUE KEY uniq_order_id (order_id)
);
```

---

### 6. Creating Payment Links (Checkout Flow)

When a user chooses to pay with card via NyvaPay:

1. **Validate** the requested amount (min / max, numeric, etc.).
2. **Create a pending invoice** in your billing table:
   - `status = 'pending'`
   - `payment_method = 'card'`
   - `transaction_type = 'credit'` (if you track debits vs credits)
3. Build a deterministic `order_id` that can be parsed later in the webhook. A common pattern is:

   ```
   nv-<userId>-<invoiceId>
   ```

4. Call NyvaPay to create a payment link:

   ```js
   const client = nyvaPayClient();
   const publicBaseUrl = process.env.PUBLIC_BASE_URL;

   const payload = {
     amount,
     currency: 'USD',
     product_name: 'Account Refill',
     note: `Order ${orderId}`,          // helps if webhook only sends back 'note'
     customer_email: user.email,
     customer_name: user.displayName,   // whatever you use
     webhook_url: process.env.NYVAPAY_WEBHOOK_URL,
     success_redirect_url: `${publicBaseUrl}/billing/success?invoiceId=${invoiceId}`,
   };

   const { data } = await client.post('/merchant/payment-links', payload);
   ```

5. NyvaPays response field names can vary. Extract URL and link ID defensively:

   ```js
   const paymentUrl =
     data.pay_url ||
     data.payment_url ||
     data.url ||
     data.checkout_url ||
     data.link;

   const paymentLinkId =
     data.payment_request_id ||
     data.id ||
     data.payment_link_id ||
     data.reference ||
     null;
   ```

6. If **no URL** is present:
   - Mark your invoice as `failed`.
   - Store `error` details.
   - Return an error to the client.

7. Insert a row into `nyvapay_payments` with:
   - `user_id`, `order_id`, `payment_link_id`, `amount`, `currency`, `status='pending'`, `credited=0`, `raw_payload`.

8. Return `paymentUrl` to the frontend so it can redirect or open it.

---

### 7. Handling the NyvaPay Webhook

Configure your NyvaPay account to POST events to `NYVAPAY_WEBHOOK_URL`, which should be a public route in this project, e.g.:

```text
POST https://yourdomain.com/webhooks/nyvapay
```

The handler must:

1. **Parse `order_id`** from the payload. Look at multiple places:
   - `payload.order_id`
   - `payload.orderId`
   - `payload.metadata.order_id`
   - `payload.note` (if you embed `Order nv-...` in the note)

2. Validate the format, e.g. `nv-<userId>-<invoiceId>`, and parse both IDs.

3. Normalize NyvaPays status into three buckets:

   - **Success**: `paid`, `completed`, `success`, `succeeded`
   - **Pending**: `pending`, `processing`, `awaiting_payment`, `in_progress`
   - **Failure**: `failed`, `cancelled`, `canceled`, `expired`, `error`, `refunded`, `chargeback`

4. **Upsert** a row into `nyvapay_payments` using `order_id` as the unique key:
   - Update `status`, `nyvapay_payment_id`, `amount`, etc.
   - Store the raw payload in `raw_payload`.

5. If state is **pending**:
   - Update status and return 200 with `{ pending: true }`.

6. If state is **failure**:
   - Mark the invoice in your billing table as `failed`, and record `error` details (provider, payload).
   - Keep `credited = 0` in `nyvapay_payments`.
   - Return 200 with `{ failed: true }`.

7. If state is **success**, implement **idempotent crediting** using `credited`:

   - Try to acquire a lock:

     ```sql
     UPDATE nyvapay_payments
     SET credited = 2, status = 'PROCESSING'
     WHERE order_id = ? AND credited = 0;
     ```

   - If `affected_rows = 0`:
     - Check if `credited = 1` → already credited, just return `{ alreadyCredited: true }`.
     - Otherwise treat as `{ pending: true }` or log an error.

   - If you acquired the lock:
     - Load the matching invoice from your billing table and verify `user_id`.
     - If invoice is already `completed`, mark `credited = 1` and return `{ alreadyCredited: true }`.
     - Otherwise:
       - Run your own complete invoice logic (update invoice status, user balance, etc.).
       - Mark `nyvapay_payments.credited = 1` and update `status`.
       - Optionally send a receipt email.

This locking pattern ensures that if NyvaPay retries the webhook or you receive the event twice, the user balance is **never** credited more than once.

---

### 8. Admin & Reporting (Optional)

With `nyvapay_payments` populated, you can easily build reporting endpoints:

- **Total NyvaPay revenue**:

  ```sql
  SELECT COALESCE(SUM(amount), 0) AS total, COUNT(*) AS count
  FROM nyvapay_payments
  WHERE status = 'COMPLETED' AND credited = 1;
  ```

- **Pending NyvaPay payments**:

  ```sql
  SELECT COUNT(*) AS count
  FROM nyvapay_payments
  WHERE status = 'pending';
  ```

You can then expose these metrics in an `/admin/stats` endpoint or similar.

---

### 9. Testing Checklist

Before relying on NyvaPay in production, verify:

- [ ] Creating a payment link returns a valid URL.
- [ ] Successful payment:
  - [ ] Invoice transitions from `pending` → `completed`.
  - [ ] User balance increases.
  - [ ] `nyvapay_payments.credited = 1`.
- [ ] Failed / canceled payment:
  - [ ] Invoice status becomes `failed`.
  - [ ] Error details are stored.
- [ ] Replaying the same webhook does **not** double-credit the user.
- [ ] Admin / reporting views show NyvaPay totals and pending counts correctly.

---

### 10. Security Notes

- Never hard‑code `NYVAPAY_API_KEY` in code; always use environment variables or a secret manager.
- If NyvaPay supports webhook signatures or IP whitelisting, enable those and verify the signature in your webhook handler.
- Log enough data to debug (especially `order_id` and status) but avoid logging full sensitive payloads in production logs.
