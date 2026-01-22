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

		{
			name: 'aihubi',
			use: {
				...devices['Desktop Chrome'],
				baseURL: 'https://aihubi.tail22dc1.ts.net'
			},
			workers: 1
		},

		{
			name: 'akabox',
			use: {
				...devices['Desktop Chrome'],
				baseURL: 'https://akabox.tail22dc1.ts.net'
			},
			workers: 1
		},

		{
			name: 'instance2',
			use: {
				...devices['Desktop Chrome'],
				baseURL: 'https://instance2.example.com'
			},
			workers: 1
		},

		{
			name: 'instance3',
			use: {
				...devices['Desktop Chrome'],
				baseURL: 'https://instance3.example.com'
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
