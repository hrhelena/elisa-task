// Starts Swagger UI (#54, ADR 0016). Ours, not vendored.
// - url is relative to /docs/, so the spec is always read from the page's own origin.
// - validatorUrl: null, or Swagger UI would send the spec to the public online validator.
// - BaseLayout with the apis preset only: the standalone preset (top bar) is not vendored.
window.ui = SwaggerUIBundle({
  url: "../openapi.json",
  dom_id: "#swagger-ui",
  deepLinking: true,
  tryItOutEnabled: true,
  validatorUrl: null,
  layout: "BaseLayout",
  presets: [SwaggerUIBundle.presets.apis],
});
