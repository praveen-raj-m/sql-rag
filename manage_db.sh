#!/bin/bash
# Database Management Tool Wrapper Script
# This script provides a simple interface to run the various database management tools

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Print banner
echo -e "${BLUE}======================================${NC}"
echo -e "${BLUE}  SQL RAG Dashboard - DB Manager     ${NC}"
echo -e "${BLUE}======================================${NC}"
echo ""

# Function to show menu
show_menu() {
    echo -e "${GREEN}Available options:${NC}"
    echo "1. List tables"
    echo "2. Delete a table"
    echo "3. Create users table"
    echo "4. Refresh schema"
    echo "5. Launch table manager UI"
    echo "6. Quit"
    echo ""
    echo -n "Enter your choice [1-6]: "
}

# Main loop
while true; do
    show_menu
    read choice
    
    case $choice in
        1)
            echo -e "\n${YELLOW}Listing tables...${NC}"
            python3 db_manager.py list
            ;;
        2)
            echo -n -e "\n${YELLOW}Enter table name to delete: ${NC}"
            read table_name
            echo -e "${YELLOW}Deleting table $table_name...${NC}"
            python3 db_manager.py delete "$table_name"
            ;;
        3)
            echo -e "\n${YELLOW}Creating users table...${NC}"
            python3 db_manager.py create-users
            ;;
        4)
            echo -e "\n${YELLOW}Refreshing schema...${NC}"
            python3 db_manager.py refresh-schema
            ;;
        5)
            echo -e "\n${YELLOW}Launching table manager UI...${NC}"
            echo -e "${YELLOW}Press Ctrl+C to return to this menu when done${NC}"
            python3 table_manager_app.py
            ;;
        6)
            echo -e "\n${GREEN}Goodbye!${NC}"
            exit 0
            ;;
        *)
            echo -e "\n${RED}Invalid choice. Please try again.${NC}"
            ;;
    esac
    
    echo ""
    echo -e "${BLUE}Press Enter to continue...${NC}"
    read
    clear
done 