import { defineConfig, devices } from '@playwright/test';

/**
 * @see https://playwright.dev/docs/test-configuration
 */
export default defineConfig({
	testDir: './tests/e2e',
	/* Run tests in files in parallel */
	fullyParallel: true,
	/* Fail the build on CI if you accidentally left test.only in the source code. */
	forbidOnly: !!process.env.CI,
	/* Retry on CI only */
	retries: process.env.CI ? 2 : 0,
	/* Let projects run in parallel, each with default workers */
	// workers: 1, // Commented out to allow default per-project workers
	/* Reporter to use. See https://playwright.dev/docs/test-reporters */
	reporter: [
		['html'],
		['json', { outputFile: 'test-results/results.json' }],
		['junit', { outputFile: 'test-results/results.xml' }]
	],
	/* Shared settings for all the projects below. See https://playwright.dev/docs/api/class-testoptions. */
	use: {
		/* Base URL to use in actions like `await page.goto('/')`. */
		baseURL: 'http://localhost:3000',

		/* Collect trace when retrying the failed test. See https://playwright.dev/docs/trace-viewer */
		trace: 'on-first-retry',

		/* Take screenshot on failure */
		screenshot: 'only-on-failure',

		/* Record video on failure */
		video: 'retain-on-failure'
	},

		/* Configure projects for major browsers */
		projects: [
			{
				name: 'chromium',
				use: { ...devices['Desktop Chrome'] },
				workers: 1
			},

			/* Test against mobile viewports. */
			{
				name: 'Mobile Chrome',
				use: { ...devices['Pixel 5'] },
				workers: 1
			},

			// Environment-specific projects for smoke testing
			{
				name: 'localhost',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_LOCALHOST || 'http://cyber24:3000'
				},
				workers: 1
			},
			{
				name: 'akabox',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_AKABOX || 'https://ai.tail22dc1.ts.net'
				},
				workers: 1
			},
			{
				name: 'akaboxdot',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_AKABOXDOT || 'https://akabox.open-webui.dgstage.se'
				},
				workers: 1
			},
			{
				name: 'preprod',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_PREPROD || 'https://preprod.open-webui.dgstage.se'
				},
				workers: 1
			},
			{
				name: 'axontech',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_AXONTECH || 'https://axontech.open-webui.dgstage.se'
				},
				workers: 1
			},
			{
				name: 'chat',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_CHAT || 'https://chat.privatestack.ai'
				},
				workers: 1
			},
			{
				name: 'dex',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_DEX || 'https://digifi-gpt.digitalist.tools'
				},
				workers: 1
			},
			{
				name: 'dotab',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_DOTAB || 'https://gpt.digitalist.tools'
				},
				workers: 1
			},
			{
				name: 'granges',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_GRANGES || 'https://granges.open-webui.dgstage.se'
				},
				workers: 1
			},
			{
				name: 'grow',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_GROW || 'https://grow.open-webui.dgstage.se'
				},
				workers: 1
			},
			{
				name: 'hsr',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_HSR || 'https://assistent.hsr.se/'
				},
				workers: 1
			},
			{
				name: 'northbound',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_NORTHBOUND || 'https://northbound.open-webui.dgstage.se'
				},
				workers: 1
			},
			{
				name: 'resiliencebot',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_RESILIENCEBOT || 'https://resiliencebot.open-webui.dgstage.se'
				},
				workers: 1
			},
			{
				name: 'sanda',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_SANDA || 'https://sanda.open-webui.dgstage.se'
				},
				workers: 1
			},
			{
				name: 'stage',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_STAGE || 'https://sanda.open-webui.dgstage.se'
				},
				workers: 1
			},
			{
				name: 'ur',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_UR || 'https://aiportalen.ur.se'
				},
				workers: 1
			},
			{
				name: 'ekn',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_EKN || 'https://ai.ekn.se'
				},
				workers: 1
			},
			{
				name: 'msb',
				use: {
					...devices['Desktop Chrome'],
					baseURL: process.env.CYPRESS_TEST_URL_MSB || 'https://msb.open-webui.dgstage.se/'
				},
				workers: 1
			}
		],

	/* Run your local dev server before starting the tests */
	webServer: {
		command: 'npm run dev',
		url: 'http://localhost:3000',
		reuseExistingServer: !process.env.CI,
		timeout: 120 * 1000
	},

	/* Global test timeout */
	timeout: 60 * 1000,

	/* Expect timeout */
	expect: {
		timeout: 10 * 1000
	}
});
