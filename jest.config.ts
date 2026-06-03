import type { Config } from 'jest';

const config: Config = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: ['**/__tests__/**/*.test.ts'],
  // Runs before each test file, before the module registry is set up.
  // Used to configure environment variables before any imports resolve.
  setupFiles: ['<rootDir>/src/__tests__/setup.ts'],
};

export default config;
