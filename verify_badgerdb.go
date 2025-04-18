package main

import (
	"fmt"
	"gohustle/badgerdb"
	"log"
)

func main() {
	// Create a new instance of BadgerDBHelper
	helper, err := badgerdb.NewBadgerDBHelper()
	if err != nil {
		log.Fatalf("Failed to create BadgerDBHelper: %v", err)
	}
	defer helper.Close()

	// Test 1: Store a credential
	fmt.Println("\n1. Testing StoreCredential:")
	err = helper.StoreCredential("test_key", "test_value")
	if err != nil {
		log.Printf("Failed to store credential: %v", err)
	} else {
		fmt.Println("✓ Credential stored successfully")
	}

	// Test 2: Get the stored credential
	fmt.Println("\n2. Testing GetCredential:")
	cred, err := helper.GetCredential("test_key")
	if err != nil {
		log.Printf("Failed to get credential: %v", err)
	} else {
		fmt.Printf("✓ Retrieved credential: %+v\n", cred)
	}

	// Test 3: Update the credential
	fmt.Println("\n3. Testing UpdateCredential:")
	err = helper.UpdateCredential("test_key", "updated_value")
	if err != nil {
		log.Printf("Failed to update credential: %v", err)
	} else {
		fmt.Println("✓ Credential updated successfully")
	}

	// Test 4: List all credentials
	fmt.Println("\n4. Testing ListCredentials:")
	keys, err := helper.ListCredentials()
	if err != nil {
		log.Printf("Failed to list credentials: %v", err)
	} else {
		fmt.Printf("✓ Found credentials: %v\n", keys)
	}

	// Test 5: Try to get a non-existent credential
	fmt.Println("\n5. Testing GetCredential (non-existent):")
	nonExistentCred, err := helper.GetCredential("non_existent_key")
	if nonExistentCred == nil && err == nil {
		fmt.Println("✓ Correctly handled non-existent credential")
	} else {
		fmt.Println("Unexpected result for non-existent credential")
	}

	// Test 6: Delete the credential
	fmt.Println("\n6. Testing DeleteCredential:")
	err = helper.DeleteCredential("test_key")
	if err != nil {
		log.Printf("Failed to delete credential: %v", err)
	} else {
		fmt.Println("✓ Credential deleted successfully")
	}

	// Verify deletion
	fmt.Println("\n7. Verifying deletion:")
	deletedCred, err := helper.GetCredential("test_key")
	if deletedCred == nil && err == nil {
		fmt.Println("✓ Credential was successfully deleted")
	} else {
		fmt.Println("Credential still exists after deletion")
	}
}
