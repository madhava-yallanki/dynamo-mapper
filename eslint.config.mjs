import { eslintConfig } from '@madhava-yallanki/ts-tools';

export default eslintConfig({
  files: ['lib/**/*.ts'],
  tsconfigRootDir: import.meta.dirname,
});
