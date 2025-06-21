#!/usr/bin/env python3
"""
Test script for actors_titles table implementation
Tests the ETL process from temp_netflix_titles to actors_titles table
"""

from controllers.actors_titles_controller import ActorsTitlesController

def test_actors_titles_implementation():
    print("🎬 Testing Actors-Titles Implementation")
    print("=" * 60)
    
    # Initialize actors-titles controller
    controller = ActorsTitlesController()
    
    # Test 1: Create temp_actors_titles table
    print("\n📋 Test 1: Creating temp_actors_titles table...")
    try:
        controller.create_temp_actors_titles_table()
        print("✅ temp_actors_titles table created successfully")
    except Exception as e:
        print(f"❌ Error creating temp_actors_titles table: {e}")
        return
    
    # Test 2: Check processing status
    print("\n📊 Test 2: Checking processing status...")
    try:
        controller.check_processing_status()
        print("✅ Processing status check successful")
    except Exception as e:
        print(f"❌ Error checking processing status: {e}")
    
    # Test 3: Process a small batch to verify logic
    print("\n🔄 Test 3: Processing a small batch (10 records)...")
    try:
        controller.populate_actors_titles_table_from_temp(batch_size=10)
        print("✅ Small batch processing completed successfully")
    except Exception as e:
        print(f"❌ Error in small batch processing: {e}")
        return
    
    # Test 4: Check status after small batch
    print("\n📊 Test 4: Status check after small batch...")
    try:
        controller.check_processing_status()
        print("✅ Status check after batch successful")
    except Exception as e:
        print(f"❌ Error checking status after batch: {e}")
    
    # Test 5: Process remaining records
    print("\n🔄 Test 5: Processing remaining records...")
    try:
        controller.populate_actors_titles_table_from_temp(batch_size=500)
        print("✅ Full processing completed successfully")
    except Exception as e:
        print(f"❌ Error in full processing: {e}")
        return
    
    # Test 6: Final status check
    print("\n📊 Test 6: Final status check...")
    try:
        controller.check_processing_status()
        print("✅ Final status check successful")
    except Exception as e:
        print(f"❌ Error in final status check: {e}")
    
    print("\n🎉 Actors-Titles Implementation Test Complete!")
    print("=" * 60)

if __name__ == "__main__":
    test_actors_titles_implementation()
