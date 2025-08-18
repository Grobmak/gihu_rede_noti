module.exports = {
  env: { node: true, es2021: true },
  extends: ["standard","prettier"],
  parserOptions: { ecmaVersion: "latest", sourceType: "module" },
  rules: {
    "promise/param-names":"error",
    "no-irregular-whitespace":"error",
    "no-case-declarations":"off",    // ปิดก่อน ถ้าจะเปิดให้ห่อ {} ตามข้อ 4
    "camelcase":"off",
    "no-unused-vars":["warn",{"argsIgnorePattern":"^_","varsIgnorePattern":"^_"}],
    "no-console":"off"
  }
};

