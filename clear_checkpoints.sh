#!/bin/bash
# Clear checkpoints to force recomputation

CHECKPOINT_DIR="traffic_checkpoints"

echo "Checkpoint Management Tool"
echo "=========================="
echo ""
echo "Available checkpoints in $CHECKPOINT_DIR/:"
echo ""

if [ -d "$CHECKPOINT_DIR" ]; then
    ls -lh "$CHECKPOINT_DIR/" 2>/dev/null || echo "  (empty)"
    echo ""
else
    echo "  No checkpoint directory found."
    echo ""
    exit 0
fi

echo "What would you like to do?"
echo "  1) Clear ALL checkpoints (full recomputation)"
echo "  2) Clear only statistics checkpoints (keep valid_data)"
echo "  3) Clear only valid_data checkpoint"
echo "  4) Cancel"
echo ""
read -p "Enter choice [1-4]: " choice

case $choice in
    1)
        echo ""
        read -p "Are you sure you want to delete ALL checkpoints? [y/N]: " confirm
        if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
            rm -rf "$CHECKPOINT_DIR"
            echo "✓ All checkpoints cleared."
        else
            echo "Cancelled."
        fi
        ;;
    2)
        echo ""
        read -p "Clear statistics checkpoints but keep valid_data? [y/N]: " confirm
        if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
            rm -rf "$CHECKPOINT_DIR/district_stats"
            rm -rf "$CHECKPOINT_DIR/road_stats"
            rm -rf "$CHECKPOINT_DIR/lane_stats"
            rm -rf "$CHECKPOINT_DIR/hourly_stats"
            rm -rf "$CHECKPOINT_DIR/dow_stats"
            echo "✓ Statistics checkpoints cleared (valid_data preserved)."
        else
            echo "Cancelled."
        fi
        ;;
    3)
        echo ""
        read -p "Clear valid_data checkpoint? [y/N]: " confirm
        if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
            rm -rf "$CHECKPOINT_DIR/valid_data"
            echo "✓ Valid_data checkpoint cleared."
        else
            echo "Cancelled."
        fi
        ;;
    4)
        echo "Cancelled."
        ;;
    *)
        echo "Invalid choice."
        ;;
esac
