module.exports = {
  env: { node: true, es2021: true },
  extends: ["standard","prettier"],
  parserOptions: { ecmaVersion: "latest", sourceType: "module" },
  rules: {
    "no-irregular-whitespace": "off",
    "no-case-declarations": "off",
    "camelcase": "off",
    "no-unused-vars": ["warn", { "argsIgnorePattern": "^_", "varsIgnorePattern": "^_" }],
    "no-console": "off"
  }
};
