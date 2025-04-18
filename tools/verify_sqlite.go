package main

import (
	"fmt"
	"log"
	"time"

	"gohustle/db"
)

func main() {
	// Initialize SQLite helper using singleton pattern
	helper, err := db.GetSQLiteHelper()
	if err != nil {
		log.Fatalf("Failed to create SQLite helper: %v", err)
	}
	defer helper.Close()

	// Test 1: Store a credential
	fmt.Println("\n=== Test 1: Store Credential ===")
	err = helper.StoreCredential("test_key", "test_value")
	if err != nil {
		log.Fatalf("Failed to store credential: %v", err)
	}
	fmt.Println("✅ Successfully stored credential")

	// Test 2: Retrieve the stored credential
	fmt.Println("\n=== Test 2: Get Credential ===")
	cred, err := helper.GetCredential("test_key")
	if err != nil {
		log.Fatalf("Failed to get credential: %v", err)
	}
	if cred == nil {
		log.Fatal("Credential not found")
	}
	fmt.Printf("✅ Retrieved credential: key=%s, value=%s\n", cred.Key, cred.Value)

	// Test 3: Update existing credential
	fmt.Println("\n=== Test 3: Update Credential ===")
	err = helper.StoreCredential("test_key", "updated_value")
	if err != nil {
		log.Fatalf("Failed to update credential: %v", err)
	}

	// Verify update
	cred, err = helper.GetCredential("test_key")
	if err != nil {
		log.Fatalf("Failed to get updated credential: %v", err)
	}
	if cred.Value != "updated_value" {
		log.Fatalf("Update failed: expected 'updated_value', got '%s'", cred.Value)
	}
	fmt.Printf("✅ Updated credential: key=%s, new_value=%s\n", cred.Key, cred.Value)

	// Test 4: Store multiple credentials
	fmt.Println("\n=== Test 4: Store Multiple Credentials ===")
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		if err := helper.StoreCredential(k, v); err != nil {
			log.Fatalf("Failed to store credential %s: %v", k, err)
		}
	}
	fmt.Println("✅ Successfully stored multiple credentials")

	// Test 5: List all credentials
	fmt.Println("\n=== Test 5: List All Credentials ===")
	keys, err := helper.ListCredentials()
	if err != nil {
		log.Fatalf("Failed to list credentials: %v", err)
	}
	fmt.Printf("✅ Found %d credentials:\n", len(keys))
	for _, key := range keys {
		cred, err := helper.GetCredential(key)
		if err != nil {
			log.Fatalf("Failed to get credential %s: %v", key, err)
		}
		fmt.Printf("   - %s: %s (Created: %s, Updated: %s)\n",
			cred.Key, cred.Value,
			cred.CreatedAt.Format(time.RFC3339),
			cred.UpdatedAt.Format(time.RFC3339))
	}

	// Test 6: Delete a credential
	fmt.Println("\n=== Test 6: Delete Credential ===")
	err = helper.DeleteCredential("key2")
	if err != nil {
		log.Fatalf("Failed to delete credential: %v", err)
	}

	// Verify deletion
	cred, err = helper.GetCredential("key2")
	if err != nil {
		log.Fatalf("Error checking deleted credential: %v", err)
	}
	if cred != nil {
		log.Fatal("Credential still exists after deletion")
	}
	fmt.Println("✅ Successfully deleted credential")

	// Test 7: Verify timestamps
	fmt.Println("\n=== Test 7: Verify Timestamps ===")
	key := "timestamp_test"
	beforeStore := time.Now()
	err = helper.StoreCredential(key, "test_value")
	if err != nil {
		log.Fatalf("Failed to store credential: %v", err)
	}

	cred, err = helper.GetCredential(key)
	if err != nil {
		log.Fatalf("Failed to get credential: %v", err)
	}

	if cred.CreatedAt.Before(beforeStore) {
		log.Fatal("CreatedAt timestamp is incorrect")
	}
	if cred.UpdatedAt.Before(beforeStore) {
		log.Fatal("UpdatedAt timestamp is incorrect")
	}
	fmt.Println("✅ Timestamps are working correctly")

	fmt.Println("\n✨ All tests passed successfully!")
}
