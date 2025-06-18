#!/usr/bin/env python3
"""
Test script for actors table implementation
"""

from controllers.actors_controller import ActorsController

def test_actors_implementation():
    print("🎬 Testing Actors Implementation")
    print("=" * 50)
    
    # Initialize actors controller
    actors_controller = ActorsController()
    
    # Test 1: Create temp_actors table
    print("\n📋 Test 1: Creating temp_actors table...")
    try:
        actors_controller.create_temp_actors_table()
        print("✅ temp_actors table created successfully")
    except Exception as e:
        print(f"❌ Error creating temp_actors table: {e}")
        return
    
    # Test 2: Check processing status
    print("\n📊 Test 2: Checking processing status...")
    try:
        status = actors_controller.check_processing_status()
        if status:
            print("✅ Processing status check successful")
        else:
            print("⚠️ Processing status returned None")
    except Exception as e:
        print(f"❌ Error checking processing status: {e}")
    
    # Test 3: Process a small batch
    print("\n🔄 Test 3: Processing a small batch...")
    try:
        actors_controller.populate_actors_table_from_temp(batch_size=10)
        print("✅ Batch processing completed successfully")
    except Exception as e:
        print(f"❌ Error in batch processing: {e}")
    
    # Test 4: Final status check
    print("\n📊 Test 4: Final status check...")
    try:
        final_status = actors_controller.check_processing_status()
        if final_status:
            print("✅ Final status check successful")
            print(f"   Completion: {final_status['completion_percentage']}%")
        else:
            print("⚠️ Final status returned None")
    except Exception as e:
        print(f"❌ Error in final status check: {e}")

if __name__ == "__main__":
    test_actors_implementation()
