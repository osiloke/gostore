package sharded_test

import (
	"fmt"
	"log"

	"github.com/osiloke/gostore/stores/memory"
	"github.com/osiloke/gostore/stores/sharded"
)

// User represents a user in our system.
type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func Example() {
	numShards := 3
	storeType := "memory" // Define the type of store we are using

	// 1. Create the GenericShardedStore with default locator and creator
	infoStore := memory.NewMemoryStore()
	shardedStore := sharded.NewGenericShardedStore(numShards, storeType, nil, nil, infoStore)
	defer shardedStore.Close()

	// 4. Use the sharded store
	log.Println("--- Storing User 1 in 'users' collection ---")
	user1 := User{ID: "user-abc-123", Name: "Alice", Email: "alice@example.com"}
	if _, err := shardedStore.Save(user1.ID, "users", user1); err != nil {
		log.Fatalf("Failed to set user1: %v", err)
	}

	log.Println("\n--- Storing User 2 in 'users_temp' collection ---")
	user2 := User{ID: "user-def-456", Name: "Bob", Email: "bob@example.com"}
	// This call will trigger the creator for a different shard
	if _, err := shardedStore.Save(user2.ID, "users_temp", user2); err != nil {
		log.Fatalf("Failed to set user2: %v", err)
	}

	log.Println("\n--- Retrieving User 1 ---")
	var retrievedUser User
	if err := shardedStore.Get(user1.ID, "users", &retrievedUser); err != nil {
		log.Fatalf("Failed to get user1: %v", err)
	}
	fmt.Printf("Successfully retrieved user: %+v\n", retrievedUser)

	log.Println("\n--- Getting Shard Info ---")
	info, err := shardedStore.GetShardInfo("shard-0")
	if err != nil {
		log.Fatalf("Failed to get shard info: %v", err)
	}
	log.Printf("Shard 0 info: %+v", info)

	log.Println("\n--- Getting All Shard Info ---")
	allInfo, err := shardedStore.GetAllShardInfo()
	if err != nil {
		log.Fatalf("Failed to get all shard info: %v", err)
	}
	log.Printf("All shard info: %+v", allInfo)

	log.Println("\n--- Getting Collection Info ---")
	collectionInfo, err := shardedStore.GetCollectionInfo("users")
	if err != nil {
		log.Fatalf("Failed to get collection info: %v", err)
	}
	log.Printf("Users collection info: %+v", collectionInfo)

	// Output:
	// Successfully retrieved user: {ID:user-abc-123 Name:Alice Email:alice@example.com}
}
