import { test as base, expect } from '@playwright/test';

// Test user configuration (matching Cypress setup)
export const adminUser = {
	name: 'akabox',
	email: 'aka@aka.com',
	password: 'qwer1234'
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

		// Wait for redirect to home page
		await this.page.waitForURL('**/');
		// The search input is in a modal that must be opened first, so we just verify basic page elements are loaded
		await expect(this.page.getByRole('button', { name: 'User Menu' })).toBeVisible();
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

		const authPage = new AuthPage(page);
		await authPage.login(adminUser.email, adminUser.password);

		await use(page);

		await context.close();
	}
});

export { expect } from '@playwright/test';
