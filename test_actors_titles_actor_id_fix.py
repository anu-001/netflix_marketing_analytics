#!/usr/bin/env python3
"""
Test script for actors_titles implementation with actor_id fix
"""

from controllers.actors_titles_controller import ActorsTitlesController


def test_actors_titles_with_actor_id_fix():
    """
    Test the actors_titles controller implementation with proper actor_id usage
    """
    print("🧪 Testing Actors-Titles Implementation (Actor ID Fix)")
    print("=" * 65)
    
    # Initialize controller
    controller = ActorsTitlesController()
    
    try:
        # Step 1: Create temp table
        print("\n1️⃣ Creating temp_actors_titles table...")
        controller.create_temp_actors_titles_table()
        
        # Step 2: Check processing status
        print("\n2️⃣ Checking processing status...")
        controller.check_processing_status()
        
        # Step 3: Process a small batch for testing (with actor_id fix)
        print("\n3️⃣ Processing actors-titles relationships (batch size 3 for testing)...")
        controller.populate_actors_titles_table_from_temp(batch_size=3)
        
        # Step 4: Final status check
        print("\n4️⃣ Final processing status...")
        controller.check_processing_status()
        
        print("\n✅ Actors-titles implementation test with actor_id fix completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    test_actors_titles_with_actor_id_fix()
