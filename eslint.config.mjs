import eslint from "@eslint/js";
import tseslint from "typescript-eslint";
import prettierPlugin from "eslint-plugin-prettier/recommended";
import importPlugin from "eslint-plugin-import";

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommendedTypeChecked,
  prettierPlugin,
  importPlugin.flatConfigs.recommended,
  {
    files: ["lib/**/*.ts"],
    languageOptions: {
      parser: tseslint.parser,
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
        ecmaVersion: "latest",
        sourceType: "module",
      },
    },
    rules: {
      "import/extensions": ["error", "ignorePackages"],
      "import/no-unresolved": "off",
      "@typescript-eslint/require-await": "off",
    },
  }
);
