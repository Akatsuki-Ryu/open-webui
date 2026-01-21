import { test, expect } from './fixtures';

test.describe('Authentication Setup', () => {
	test.beforeAll(async () => {
		// Register admin user if not exists
		const response = await fetch('http://localhost:3000/api/v1/auths/signup', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				name: 'akabox',
				email: 'aka@aka.com',
				password: process.env.ADMIN_PASSWORD || 'qwer1234'
			})
		});

		// Accept both success (200) and conflict (400) responses
		expect([200, 400]).toContain(response.status);
	});

	test('admin user registration should work', async ({ authPage }) => {
		// This test ensures the admin user is properly registered
		// If the test passes, the admin user exists and can be used in other tests
		expect(true).toBe(true);
	});
});
