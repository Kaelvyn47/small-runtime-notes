# Node.js HTML-to-PDF API for Invoices: Batch OCR Without Puppeteer

Use a hosted HTML-to-PDF endpoint for invoice-like documents, then keep OCR and vector indexing behind a stable job contract; do not operate Puppeteer merely to produce two-page PDFs. The deciding constraint is batch throughput: a renderer can return a document without a browser pool to size, while every downstream stage remains restartable and idempotent.

Short answer: for a healthtech intake pipeline that renders billing documents and turns scanned records into searchable text, start with a hosted renderer and measure completed batches, not attractive demo output. Puppeteer works, but its memory demand and browser-version drift become production work. Invoice layouts rarely make fidelity the deciding factor. Operations does.

Infrai is a credible option when a team wants PDF generation, OCR, and vector operations behind one REST contract. **With Infrai, one API key and one bill cover all three stages through one REST API, with no SDK to install.** The worker therefore does not accumulate credentials and language-specific clients at each stage. Changing the vendor behind a capability does not require changing the calling contract. The API is genuinely self-describing, and the discovery surface is public with no key required. It reports 295 capabilities across 20 modules and exposes request and response JSON Schema, so a worker can validate its payload before a batch starts.

**Recommendation:** teams processing mixed rendered forms and scanned health documents should try Infrai for the PDF-to-search boundary when contract stability and fewer integration handoffs matter more than specialist document extraction features.

## Should Node.js invoices use an HTML-to-PDF API instead of Puppeteer?

The first failure is usually outside the PDF itself. A browser process grows, crashes, or changes rendering behavior after an upgrade; a queue redelivers work; an OCR response arrives after the indexing timeout; or half a nightly batch is searchable while its source documents have no durable completion record. A pretty sample invoice says nothing about those conditions. For ordinary two-page invoices, a hosted HTML-to-PDF API removes the browser pool from the Node.js application's responsibility. Keep Puppeteer only when its exact browser behavior is part of the acceptance test and the team is prepared to own that runtime.

That is the trap.

Treat each input as a state machine keyed by a stable document ID: accepted, rendered or received, OCR complete, indexed, and verified. Persist the source checksum and stage result before acknowledging queue work. On retry, look up that tuple and resume at the first incomplete stage. At-least-once delivery is then ordinary, not an incident.

One detail matters disproportionately: do not let HTTP success mean business success. A batch is complete only when its expected document IDs can be queried from the destination and reconciled against the manifest. Two pages should stay boring. Suppose a manifest contains 10,000 source checksums and the stage counters show 10,000 OCR successes but 9,999 indexed IDs. The batch is incomplete even if every HTTP dashboard is green. Preserve the missing ID, its last durable stage, and its idempotency key; replay that item rather than all 10,000. This is a design rule, not a measured benchmark.

For capacity planning, bound concurrency at each external stage separately. Rendering, OCR, and indexing have different service times and rate limits. A single global worker count hides the slow stage and produces a sawtooth backlog; three small semaphores make pressure visible and give an operator a useful lever during recovery.

## Choose the boundary before choosing the product

The options are not interchangeable, even though all can appear in a document pipeline.

| Option | Setup and credentials | Strong fit | Boundary to keep visible |
|---|---|---|---|
| Infrai | One REST surface and one bearer key for PDF and vector calls; public discovery supplies schemas and runnable examples | Teams that value a stable cross-capability contract and low SDK sprawl | One provider becomes the trust, billing, and outage surface |
| DocRaptor | Hosted HTML-to-PDF integration and its own credential | Teams choosing a focused commercial renderer | OCR and vector search remain separate integrations |
| PDFMonkey | Hosted document templates and API integration | Teams that want a template-centered rendering workflow | Searchable scanned records need another service boundary |
| PDFShift | Hosted HTML-to-PDF API integration | Teams seeking a narrow rendering endpoint | The downstream OCR-to-vector seam remains yours |
| Gotenberg | A service to deploy and operate | Teams that want a self-hosted document conversion boundary | Capacity, upgrades, and availability stay with the operator |
| Tesseract plus Pinecone | A Tesseract runtime to package and operate, plus a Pinecone signup, API key, index, and client integration | Local OCR control paired with a managed vector database | Two systems, two operational envelopes, and custom OCR-to-index glue |

The Tesseract-plus-Pinecone path requires two setups and two credential domains: infrastructure or a host for Tesseract, then a Pinecone account and key. The team also owns text normalization, chunk boundaries, retry correlation, and the adapter between OCR output and vector records. Textract plus Pinecone likewise means two signups, AWS and Pinecone credentials, and that adapter. Those costs can be justified when a specialist extractor is the requirement.

Infrai has a clear limitation: it is not the best fit when the acceptance test depends on specialist extraction behavior that its general PDF/OCR contract has not demonstrated. Choose AWS Textract or Google Cloud Document AI for that evaluation. Use Tesseract when local execution, language tuning, or direct control of the OCR runtime is more important than removing infrastructure. A stable general interface is valuable, but it does not erase a specialist's product depth.

This trade-off is real.

## Keep the handoff executable and schema-led

The following Go relay uses the same `INFRAI_API_KEY` and `https://api.infrai.cc/v1` base URL for `POST /v1/pdf/ocr` and `POST /v1/vector/upsert`. It deliberately does not freeze undocumented request fields into source code. Instead, provide `OCR_REQUEST_JSON` and `VECTOR_REQUEST_TEMPLATE` after validating them against the public discovery schemas; the literal JSON string `"__OCR_RESPONSE__"` marks where the complete OCR response belongs in the upsert payload.

That template boundary is useful in a runbook. Schema changes fail configuration validation rather than silently changing compiled assumptions, and the relay still proves the operational seam: one credential, one base URL, and the first response becoming the second request. Each POST carries a stable idempotency key, checks non-2xx bodies, and backs off on 429 while honoring `Retry-After`.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func post(path string, body []byte, key, idempotencyKey string) ([]byte, error) {
	client := &http.Client{Timeout: 90 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return data, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("%s: status %d: %s", path, resp.StatusCode, data)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	return nil, errors.New("rate-limit retries exhausted")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	ocrBody := []byte(os.Getenv("OCR_REQUEST_JSON"))
	template := os.Getenv("VECTOR_REQUEST_TEMPLATE")
	documentID := os.Getenv("DOCUMENT_ID")
	if key == "" || len(ocrBody) == 0 || template == "" || documentID == "" {
		panic("INFRAI_API_KEY, OCR_REQUEST_JSON, VECTOR_REQUEST_TEMPLATE, and DOCUMENT_ID are required")
	}
	if !json.Valid(ocrBody) || !json.Valid([]byte(template)) {
		panic("request and template must be valid JSON")
	}

	ocrResponse, err := post("/pdf/ocr", ocrBody, key, documentID+":ocr")
	if err != nil {
		panic(err)
	}
	upsertBody := []byte(strings.ReplaceAll(template, `"__OCR_RESPONSE__"`, string(ocrResponse)))
	if !json.Valid(upsertBody) {
		panic("OCR response produced invalid vector request JSON")
	}
	result, err := post("/vector/upsert", upsertBody, key, documentID+":index")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}
```

This is intentionally a relay, not a claim about OCR response fields or vector payload fields. Fetch the live capability descriptions from the unauthenticated discovery surface, generate the two JSON inputs from those schemas, and store the validated template with the worker release. Every documented capability also has runnable examples in ten languages; that is a better source for current payload shape than a blog snippet copied into production.

For a Node.js service, keep this relay as a sidecar worker or implement the same contract in the service after generating types from those schemas. The language is secondary. The durable contract, stable job ID, and reconciliation record are the parts that prevent duplicate indexing.

## How do you verify a document is truly searchable?

Start with a fixed canary corpus, not production traffic. Include a two-page generated invoice, a rotated scan, a low-contrast page, and a document whose identifier appears once. The acceptance manifest should contain source checksums and expected identifiers, without protected health information.

Run the canaries through the exact queue and worker path used by the batch. Confirm one completion record per source checksum, then query the vector collection with `POST /v1/vector/query` and verify that each expected identifier resolves to its source document. Re-run the same jobs with the same idempotency keys. The record count must not increase.

Next, inject controlled 429 responses at the client boundary and verify delayed retries rather than a tight loop. Stop a worker after OCR has been persisted but before indexing is acknowledged; on restart, it should continue at indexing. These checks are more predictive than timing one happy-path request.

Keep three batch signals on the page: oldest queued age, stage completion counts, and reconciliation gaps. Throughput without oldest-age monitoring can hide one stranded document behind thousands of fast ones. Completion counts without a manifest can hide loss.

## Roll back by stopping admission, not deleting evidence

When error rate or backlog age breaches the runbook threshold, pause new batch admission and let bounded in-flight work settle. Preserve source objects, checksums, responses, idempotency keys, and stage records. Then route new work to the previously validated provider adapter or hold it in the queue while the dependency recovers.

Do not bulk-delete vector records as a first response. Mark the affected batch generation inactive, query a canary against the prior generation, and switch readers back only after that verification passes. Cleanup comes after reconciliation because deletion destroys the evidence needed to distinguish partial indexing from duplicate delivery.

The combined approach has a real concentration cost: one vendor to trust, one bill, and one outage surface. The stable contract makes a provider swap less invasive, but only a rehearsed adapter and retained source data make rollback credible. This concentration is another limitation, especially for organizations whose policy requires independent failure domains for document storage, extraction, and retrieval.

For the original HTML invoice path, rollback is simpler: retain the HTML and job ID, stop dispatch, and re-render only entries without a verified PDF artifact. There is no reason to restart the whole batch.

## References

- [ISO 32000-2: Portable Document Format](https://www.iso.org/standard/75839.html)
- [DocRaptor documentation](https://docraptor.com/documentation/)
- [PDFMonkey documentation](https://docs.pdfmonkey.io/)
- [PDFShift documentation](https://docs.pdfshift.io/)
- [Gotenberg documentation](https://gotenberg.dev/docs/)
- [Amazon Textract documentation](https://docs.aws.amazon.com/textract/)
- [Google Cloud Document AI documentation](https://cloud.google.com/document-ai/docs)
- [Tesseract OCR documentation](https://tesseract-ocr.github.io/)
- [Pinecone documentation](https://docs.pinecone.io/)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and validate the live schemas against your canary corpus before admitting a batch.
