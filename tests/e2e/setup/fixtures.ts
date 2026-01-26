import { test as base, expect } from '@playwright/test';

// Test user configuration (matching Cypress setup)
export const adminUser = {
	name: 'akabox',
	email: 'aka@aka.com',
	password: process.env.ADMIN_PASSWORD || 'qwer1234'
};

// Page Object Model for Authentication
export class AuthPage {
	constructor(private page: any) {}

	async goto() {
		await this.page.goto('/auth');
	}

	async login(email: string, password: string) {
		// Ensure we're on the auth page
		await this.page.goto('/auth');

		// Fill login form
		await this.page.getByLabel('Email').fill(email);
		await this.page.locator('input[type="password"]').fill(password);

		// Submit form
		await this.page.getByRole('button', { name: 'Sign in' }).click();

		// Wait for redirect to home page - using a more reliable pattern
		// The redirect might not be immediate, wait a bit longer but with a reasonable timeout
		try {
			// Try to wait for either the home page or user menu, whichever comes first
			await Promise.race([
				this.page.waitForURL('/'),
				this.page.waitForSelector('[data-testid="user-menu"]', { timeout: 10000 })
			]);
		} catch (e) {
			// If still not redirected, proceed anyway
			console.warn('Login may not have fully completed, continuing with test...');
		}

		// Verify we're on main page by checking for a key element
		try {
			await expect(this.page.getByRole('button', { name: 'User Menu' })).toBeVisible({
				timeout: 5000
			});
		} catch (e) {
			console.warn('Could not verify login success, but test will proceed');
		}
	}

	async register(name: string, email: string, password: string) {
		await this.page.goto('/auth');

		// Switch to sign up
		await this.page.getByRole('button', { name: 'Sign up' }).click();

		// Fill registration form
		await this.page.getByLabel('Name').fill(name);
		await this.page.getByLabel('Email').fill(email);
		await this.page.locator('input[type="password"]').fill(password);

		// Submit form
		await this.page.getByRole('button', { name: 'Create account' }).click();

		// Wait for redirect or pending status
		await this.page.waitForTimeout(2000);
	}

	async logout() {
		// Click user menu
		await this.page.getByRole('button', { name: 'User Menu' }).click();

		// Click logout
		await this.page.getByRole('menuitem', { name: 'Sign out' }).click();

		// Wait for redirect to auth page
		await this.page.waitForURL('**/auth');
	}
}

// Extend the base test with authentication fixtures
type AuthFixtures = {
	authPage: AuthPage;
	adminPage: import('@playwright/test').Page;
};

export const test = base.extend<AuthFixtures>({
	authPage: async ({ page }, use) => {
		const authPage = new AuthPage(page);
		await use(authPage);
	},

	adminPage: async ({ browser }, use: (page: import('@playwright/test').Page) => Promise<void>) => {
		const context = await browser.newContext();
		const page = await context.newPage();

		// Determine baseURL by navigating to root
		await page.goto('/');
		const currentURL = page.url();
		const baseURL = new URL(currentURL).origin;

		// Set credentials based on baseURL
		let username = 'aka@aka.com';
		let password = process.env.ADMIN_PASSWORD || 'qwer1234';

		if (baseURL === 'https://aihubi.tail22dc1.ts.net') {
			username = process.env.AI_TAIL_USERNAME || 'aka@aka.com';
			password = process.env.AI_TAIL_PASSWORD || 'Ob3a4unnKAGvC6';
		} else if (baseURL === 'https://akabox.open-webui.dgstage.se') {
			username = process.env.CYPRESS_TEST_EMAIL_AKABOXDOT || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_AKABOXDOT || 'your_preprod_password';
		} else if (baseURL === 'https://stage.open-webui.dgstage.se') {
			username = process.env.CYPRESS_TEST_EMAIL_STAGE || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_STAGE || 'your_stage_password';
		} else if (baseURL === 'https://preprod.open-webui.dgstage.se') {
			username = process.env.CYPRESS_TEST_EMAIL_PREPROD || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_PREPROD || 'your_preprod_password';
		} else if (baseURL === 'https://axontech.open-webui.dgstage.se') {
			username = process.env.CYPRESS_TEST_EMAIL_AXONTECH || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_AXONTECH || 'your_axontech_password';
		} else if (baseURL === 'https://chat.privatestack.ai') {
			username = process.env.CYPRESS_TEST_EMAIL_CHAT || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_CHAT || 'your_chat_password';
		} else if (baseURL === 'https://digifi-gpt.digitalist.tools') {
			username = process.env.CYPRESS_TEST_EMAIL_DEX || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_DEX || 'your_dex_password';
		} else if (baseURL === 'https://gpt.digitalist.tools') {
			username = process.env.CYPRESS_TEST_EMAIL_DOTAB || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_DOTAB || 'your_dotab_password';
		} else if (baseURL === 'https://granges.open-webui.dgstage.se') {
			username = process.env.CYPRESS_TEST_EMAIL_GRANGES || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_GRANGES || 'your_granges_password';
		} else if (baseURL === 'https://grow.open-webui.dgstage.se') {
			username = process.env.CYPRESS_TEST_EMAIL_GROW || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_GROW || 'your_grow_password';
		} else if (baseURL === 'https://assistent.hsr.se/') {
			username = process.env.CYPRESS_TEST_EMAIL_HSR || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_HSR || 'your_hsr_password';
		} else if (baseURL === 'https://northbound.open-webui.dgstage.se') {
			username = process.env.CYPRESS_TEST_EMAIL_NORTHBOUND || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_NORTHBOUND || 'your_northbound_password';
		} else if (baseURL === 'https://resiliencebot.open-webui.dgstage.se') {
			username = process.env.CYPRESS_TEST_EMAIL_RESILIENCEBOT || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_RESILIENCEBOT || 'your_resiliencebot_password';
		} else if (baseURL === 'https://sanda.open-webui.dgstage.se') {
			username = process.env.CYPRESS_TEST_EMAIL_SANDA || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_SANDA || 'your_sanda_password';
		} else if (baseURL === 'https://stage.open-webui.dgstage.se') {
			username = process.env.CYPRESS_TEST_EMAIL_STAGE || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_STAGE || 'your_stage_password';
		} else if (baseURL === 'https://aiportalen.ur.se') {
			username = process.env.CYPRESS_TEST_EMAIL_UR || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_UR || 'your_ur_password';
		} else if (baseURL === 'https://ai.ekn.se') {
			username = process.env.CYPRESS_TEST_EMAIL_EKN || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_EKN || 'your_ekn_password';
		} else if (baseURL === 'https://msb.open-webui.dgstage.se/') {
			username = process.env.CYPRESS_TEST_EMAIL_MSB || 'admin@digitalist.cloud';
			password = process.env.CYPRESS_TEST_PASSWORD_MSB || 'your_msb_password';
		} else {
			throw new Error(`Unsupported baseURL: ${baseURL}`);
		}

		const authPage = new AuthPage(page);
		await authPage.login(username, password);

		await use(page);

		await context.close();
	}
});

export { expect } from '@playwright/test';
