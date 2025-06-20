#!/usr/bin/env python3
"""
Test script for titles implementation
Tests the ETL pipeline for titles using temp_titles with processed flag
"""

import sys
import os
from controllers.titles_controller_new import TitlesController


def test_titles_etl():
    """Test the complete titles ETL pipeline"""
    print("🧪 Testing Titles ETL Pipeline")
    print("=" * 50)
    
    try:
        # Initialize controller
        controller = TitlesController()
        
        # Step 1: Create temp_titles table
        print("\n1️⃣ Creating temp_titles table...")
        controller.create_temp_titles_table()
        
        # Step 2: Check processing status
        print("\n2️⃣ Checking processing status...")
        status = controller.check_processing_status()
        
        if status and status['total'] > 0:
            print(f"✅ Found {status['total']} titles to process")
            
            # Step 3: Populate titles table (process first 5 records for testing)
            print("\n3️⃣ Populating titles table...")
            controller.populate_titles_table_from_temp()
            
            # Step 4: Final status check
            print("\n4️⃣ Final processing status...")
            final_status = controller.check_processing_status()
            
            if final_status:
                print(f"✅ Titles ETL processed {final_status['processed']} records!")
                return True
            else:
                print("⚠️ Could not get final status")
                return False
        else:
            print("❌ No titles found to process")
            return False
            
    except Exception as e:
        print(f"❌ Error during titles ETL: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main test function"""
    success = test_titles_etl()
    
    if success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
