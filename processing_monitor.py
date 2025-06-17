"""
Processing Status Monitor for Netflix Analytics
"""

from controllers.base_tracking_controller import BaseTrackingController
import sys


def main():
    """
    Monitor and display processing status of all tables
    """
    tracker = BaseTrackingController()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "dashboard":
            # Show processing dashboard
            tracker.print_processing_dashboard()
            
        elif command == "check":
            if len(sys.argv) > 2:
                table_name = sys.argv[2]
                status = tracker.get_table_status(table_name)
                if status:
                    print(f"\n📋 Latest processing status for '{table_name}':")
                    print(f"   Status: {status['status']}")
                    print(f"   Records Processed: {status['records_processed']}")
                    print(f"   Records Created: {status['records_created']}")
                    print(f"   Records Skipped: {status['records_skipped']}")
                    print(f"   Start Time: {status['start_time']}")
                    print(f"   End Time: {status['end_time']}")
                    if status['error_message']:
                        print(f"   Error: {status['error_message']}")
                else:
                    print(f"❌ No processing runs found for table '{table_name}'")
            else:
                print("❌ Please specify a table name: python processing_monitor.py check <table_name>")
                
        elif command == "status":
            # Show quick status of all tables
            print("\n🔍 QUICK STATUS CHECK")
            print("=" * 50)
            
            tables = [
                "people", "ratings", "title_types", "categories", 
                "countries", "titles", "actors", "directors"
            ]
            
            for table in tables:
                is_processed = tracker.check_if_table_processed(table)
                status_icon = "✅" if is_processed else "❌"
                latest = tracker.get_table_status(table)
                last_status = latest['status'] if latest else "never run"
                print(f"{status_icon} {table:<15} - {last_status}")
                
        else:
            print("❌ Unknown command. Use: dashboard, check, or status")
    else:
        # Default: show dashboard
        tracker.print_processing_dashboard()


if __name__ == "__main__":
    main()
