#!/usr/bin/env python3
"""
Check table structures for actors_titles implementation
"""

from sqlalchemy import create_engine, text
from config import DB_CONFIG


def check_table_structures():
    """
    Check the structure of actor_titles and related tables
    """
    print("🔍 Checking Table Structures")
    print("=" * 50)
    
    conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(conn_string)
    
    with engine.connect() as conn:
        # Check actor_titles table structure
        print("\n📋 actor_titles table structure:")
        try:
            result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'actor_titles' 
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """))
            
            for row in result:
                print(f"   {row.column_name}: {row.data_type} ({'NULL' if row.is_nullable == 'YES' else 'NOT NULL'})")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        # Check actors table structure
        print("\n👥 actors table structure:")
        try:
            result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'actors' 
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """))
            
            for row in result:
                print(f"   {row.column_name}: {row.data_type} ({'NULL' if row.is_nullable == 'YES' else 'NOT NULL'})")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            
        # Check titles table structure
        print("\n🎬 titles table structure:")
        try:
            result = conn.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'titles' 
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """))
            
            for row in result:
                print(f"   {row.column_name}: {row.data_type} ({'NULL' if row.is_nullable == 'YES' else 'NOT NULL'})")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")


if __name__ == "__main__":
    check_table_structures()
