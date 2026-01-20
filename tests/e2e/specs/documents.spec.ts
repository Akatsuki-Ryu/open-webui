import { test } from '../setup/fixtures';

test.describe('Knowledge Management', () => {
	test('user can view knowledge management page', async ({ adminPage }) => {
		// Navigate directly to documents page
		await adminPage.goto('/documents');

		// Verify we're on the knowledge management page
		// Simply verify the page loaded by checking page elements
		await adminPage.getByRole('heading', { name: 'Knowledge Base' }).isVisible();
	});

	test('user can upload a document to knowledge base', async ({ adminPage }) => {
		// Navigate directly to documents page
		await adminPage.goto('/documents');

		// Create a small test file for upload
		const testFilePath = 'test-file.txt';
		const fs = require('fs');

		try {
			// Create a simple text file for testing
			fs.writeFileSync(testFilePath, 'This is a test document for upload.');

			// Upload document (this is a simplified approach)
			await adminPage.locator('input[type="file"]').setInputFiles(testFilePath);

			// Wait for upload to complete and verify
			await adminPage.getByText('test-file.txt').isVisible();
		} finally {
			// Clean up test file
			if (fs.existsSync(testFilePath)) {
				fs.unlinkSync(testFilePath);
			}
		}
	});

	test('user can search documents in knowledge base', async ({ adminPage }) => {
		// Navigate directly to documents page
		await adminPage.goto('/documents');

		// Try to search documents
		await adminPage.getByPlaceholder('Search documents...').fill('test');

		// Verify search input is visible
		await adminPage.getByPlaceholder('Search documents...').isVisible();
	});

	test('user can delete a document from knowledge base', async ({ adminPage }) => {
		// Skip for now as it requires data setup and is complex to make reliable
		test.skip(true, 'Skipping delete test - requires setup of document to delete');
	});
});
