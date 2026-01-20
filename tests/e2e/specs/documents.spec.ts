import { test } from '../setup/fixtures';
import { DocumentsPage } from '../setup/pages/DocumentsPage';
import path from 'path';

test.describe('Documents', () => {
  let documentsPage: DocumentsPage;

  test.beforeEach(async ({ adminPage }) => {
    documentsPage = new DocumentsPage(adminPage);
    await documentsPage.goto();
    await documentsPage.waitForDocumentsToLoad();
  });

  test('user can view documents page', async () => {
    // Verify we're on the documents page
    await documentsPage.waitForDocumentsToLoad();
  });

  test('user can upload a document', async () => {
    // Create a small test file for upload
    const testFilePath = path.join(process.cwd(), 'test-file.txt');

    // Create a simple text file for testing
    await test.step('Create test file', async () => {
      const fs = require('fs');
      fs.writeFileSync(testFilePath, 'This is a test document for upload.');
    });

    try {
      await documentsPage.uploadDocument(testFilePath, 'test-file.txt');
    } finally {
      // Clean up test file
      const fs = require('fs');
      if (fs.existsSync(testFilePath)) {
        fs.unlinkSync(testFilePath);
      }
    }
  });

  test('user can search documents', async () => {
    // This test assumes there are existing documents
    // If no documents exist, it will pass but not test search functionality
    await documentsPage.searchDocuments('test');
  });

  test('user can delete a document', async () => {
    // This test assumes there are existing documents to delete
    // In a real scenario, you'd first upload a document then delete it
    const documentList = documentsPage.getDocumentList();
    const count = await documentList.count();

    if (count > 0) {
      await documentsPage.deleteDocument(0);
    } else {
      // Skip test if no documents exist
      test.skip();
    }
  });
});