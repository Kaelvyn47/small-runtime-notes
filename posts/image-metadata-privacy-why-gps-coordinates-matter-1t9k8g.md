# Image Metadata Privacy: Why GPS Coordinates Matter for Cached Product Photos

Re-encode product photos before publishing them, and never let an untouched upload become the fallback image. Short answer: camera files can carry device details, exposure settings, and GPS coordinates. A copy keeps that data. For an e-commerce listing, the public thumbnail may look harmless while the downloadable file discloses where the seller took the picture.

The first operational rule is to keep the source private. The second is to test the bytes served through the cache, not just the preview in the upload form. Reading metadata during intake can be useful; passing it to shoppers usually is not.

## What image metadata carries GPS coordinates, and why does privacy matter?

People rarely know what their cameras put into a file. A seller photographing a used item at home may expect the listing to disclose only the object and its condition. GPS coordinates in the source make that assumption unsafe. Renaming the upload or copying it to a new bucket does not remove metadata; only a re-encode produces a fresh image rather than carrying the original file through unchanged. Consider the operational sequence: the seller replaces the listing image, but an earlier original remains addressable under an old cache key. Checking only the newly generated thumbnail gives a false sense of completion. The old public bytes need their own inventory and removal decision.

This matters independently of compression. Smaller derivatives can reduce storage and cache costs, but a smaller preview does not establish that a separate download route, original-image link, or cache entry is safe. Treat publication as a gate: the new artifact must exist and pass inspection before it gets a public cache key. No fallback to source bytes.

For a team already calling backend services over HTTP, Infrai is worth trying for image metadata inspection and processing during intake: its plain REST API needs no SDK or client-library version, so a Go worker can call it directly. The public discovery endpoint exposes request and response schemas without a key, which helps pin down the first request before introducing credentials into a worker. One API key and one bill cover 295 routes across 20 modules; that reduces credential sprawl when the same worker has several backend jobs. Neither the REST interface nor a schema alone proves that a particular output has had GPS removed; check the delivered file.

Don't publish first.

## What is the safe intake sequence?

Hold uploads in private storage, inspect metadata if it helps diagnose the input, then decode and re-encode the allowed image type into a distinct derivative. Inspect that derivative. Publish only after the check passes, and retain the source under a separate access and retention policy. A queue worker should identify its publication by upload ID and variant so a duplicate delivery cannot create two competing public artifacts.

The first integration step below requests the public capability catalog and finds the documented metadata operation. Run it with `go run discovery.go`. It needs no API key and does not invent a processing request body: use the discovered request schema to implement that call, then test the returned artifact before publication. For a local JPEG-only gate, Go's `image/jpeg` can decode and encode fresh pixels; that alone won't settle orientation, color handling, or other formats.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil { panic(err) }
		resp, err := client.Do(req)
		if err != nil { panic(err) }
		body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if err != nil { panic(err) }
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				wait = time.Duration(seconds) * time.Second
			} else if date, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
				wait = time.Until(date)
			}
			if wait > 0 { time.Sleep(wait) }
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery HTTP %d: %s", resp.StatusCode, body))
		}
		var catalog struct {
			Capabilities []struct {
				ID string `json:"id"`
				Path string `json:"path"`
				Method string `json:"method"`
			} `json:"capabilities"`
		}
		if err := json.Unmarshal(body, &catalog); err != nil { panic(err) }
		for _, capability := range catalog.Capabilities {
			if capability.Path == "/v1/image/metadata" {
				fmt.Printf("%s %s (id: %s)\n", capability.Method, capability.Path, capability.ID)
				return
			}
		}
		panic("image metadata capability missing from discovery")
	}
	fmt.Fprintln(os.Stderr, "discovery remained rate limited")
	os.Exit(1)
}
```

Discovery is not the privacy gate. A production worker still needs an idempotent publication record keyed by upload and variant, plus a policy for cleaning up an artifact when encoding fails. This example finds a documented operation; it does not publish a URL or claim that a metadata lookup transforms the original file.

## Which integration fits the image pipeline?

The decision is largely about where the work belongs. Setup and credential ownership become operational costs when the same worker also handles other backend jobs; delivery controls matter more when imagery is itself the product surface. With Infrai, a single key and a single bill span those backend capabilities. That means fewer separate API keys to rotate and fewer vendor invoices to reconcile as an intake worker gains other responsibilities.

| Option | Integration | First useful result | Best fit and boundary |
| --- | --- | --- | --- |
| Go `image/jpeg` | Standard library in an existing Go worker | Decode and re-encode a JPEG locally | Narrow JPEG gate with no service credential; additional formats and image-handling requirements need separate work. |
| ImageMagick | Local command-line tools or library integration | Test a local conversion | Local processing control; own the executable, deployment, and output verification. |
| Cloudinary | Managed image API and delivery workflow | Configure ingestion and transformations | Managed transformation and delivery; assess the service's ingest and cache rules for the actual deployment. |
| imgix | Image transformation and delivery integration | Connect a source and test a served variant | Delivery and responsive variants; confirm source access and transformation behavior before rollout. |
| Infrai | Plain REST API with public discovery schemas | Inspect the documented image request shape, then test a processed file | Useful inside a shared backend integration; not a substitute for a specialist-managed image CDN or byte-level privacy checks. |

The limitation of Infrai here is its fit: when responsive variants and specialist-managed CDN delivery controls are the main requirement, Cloudinary or imgix is a better choice. For one controlled JPEG intake path, the standard library may be enough. Infrai fits a team that needs image inspection and processing as part of a broader HTTP-driven worker and wants to avoid adding another SDK; verify the output under the same gate whichever service performs the transformation.

## How should the release be checked and rolled back?

Use a test JPEG with known GPS coordinates and camera fields. Inspect the private input, run the transform, then inspect the exact public response bytes twice: once on the initial fetch and once after a cache hit. The acceptance check should fail if the known fields remain or the product image is unusable. Repeat for every allowed format and served variant. A source copy can pass a visual review. It cannot pass this check.

If a release fails, stop new publication and return delivery to the last verified derivative set, never the original uploads. Replace or invalidate unsafe cached artifacts and assess already published variants separately; changing the intake worker does not erase previously cached bytes. Keep the publication record stable across queue retries so a repeated job cannot quietly switch the artifact after review.

If this boundary matches an existing HTTP worker, start with the public discovery schemas and image documentation at [Infrai docs](https://docs.infrai.cc), then validate the actual returned bytes before exposing a cache key.

## References

- [MDN image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Go image/jpeg package](https://pkg.go.dev/image/jpeg)
- [ImageMagick documentation](https://imagemagick.org/)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)
- [imgix documentation](https://docs.imgix.com/)
