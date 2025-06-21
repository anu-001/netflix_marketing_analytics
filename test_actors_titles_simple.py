#!/usr/bin/env python3
"""
Simple test script for actors_titles table implementation
Tests just the basic functionality
"""

from controllers.actors_titles_controller import ActorsTitlesController

def simple_test():
    print("🎬 Simple Actors-Titles Test")
    print("=" * 40)
    
    controller = ActorsTitlesController()
    
    # Test 1: Check if temp table exists and create if needed
    print("\n📋 Step 1: Ensure temp_actors_titles table exists...")
    try:
        controller.create_temp_actors_titles_table()
        print("✅ temp_actors_titles table ready")
    except Exception as e:
        print(f"❌ Error: {e}")
        return
    
    # Test 2: Check processing status
    print("\n📊 Step 2: Check processing status...")
    try:
        controller.check_processing_status()
        print("✅ Status check complete")
    except Exception as e:
        print(f"❌ Error: {e}")
        return
    
    # Test 3: Process just 5 records to test the logic
    print("\n🔄 Step 3: Process 5 records as test...")
    try:
        controller.populate_actors_titles_table_from_temp(batch_size=5)
        print("✅ Test batch complete")
    except Exception as e:
        print(f"❌ Error: {e}")
        return
    
    print("\n🎉 Simple test complete!")

if __name__ == "__main__":
    simple_test()
