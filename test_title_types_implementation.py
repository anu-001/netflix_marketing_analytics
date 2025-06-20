#!/usr/bin/env python3
"""
Test script for title_types implementation
Tests the ETL pipeline for title_types using temp_title_types with processed flag
"""

import sys
import os
from controllers.title_types_controller import TitleTypesController


def test_title_types_etl():
    """Test the complete title_types ETL pipeline"""
    print("🧪 Testing Title Types ETL Pipeline")
    print("=" * 50)
    
    try:
        # Initialize controller
        controller = TitleTypesController()
        
        # Step 1: Create temp_title_types table
        print("\n1️⃣ Creating temp_title_types table...")
        controller.create_temp_title_types_table()
        
        # Step 2: Check processing status
        print("\n2️⃣ Checking processing status...")
        status = controller.check_processing_status()
        
        if status and status['total'] > 0:
            print(f"✅ Found {status['total']} title types to process")
            
            # Step 3: Populate title_types table
            print("\n3️⃣ Populating title_types table...")
            controller.populate_title_types_table_from_temp()
            
            # Step 4: Final status check
            print("\n4️⃣ Final processing status...")
            final_status = controller.check_processing_status()
            
            if final_status and final_status['completion_percentage'] == 100.0:
                print("✅ Title types ETL completed successfully!")
                return True
            else:
                print("⚠️ Title types ETL may not be fully complete")
                return False
        else:
            print("❌ No title types found to process")
            return False
            
    except Exception as e:
        print(f"❌ Error during title types ETL: {e}")
        return False


def main():
    """Main test function"""
    success = test_title_types_etl()
    
    if success:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
