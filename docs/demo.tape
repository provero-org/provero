# Provero Demo - Terminal Recording
#
# This VHS tape file generates a ~15-second demo GIF showing
# provero init and provero run with colorful table output.
#
# Requirements:
#   - charmbracelet/vhs: go install github.com/charmbracelet/vhs@latest
#   - provero must be installed and available in PATH
#
# Before running:
#   mkdir -p /tmp/provero-demo && cd /tmp/provero-demo
#   printf 'order_id,customer_id,amount,status\n1,101,250.00,pending\n2,102,89.99,shipped\n3,103,450.00,delivered\n4,104,32.50,pending\n5,105,999.00,cancelled\n' > orders.csv
#   cat > provero.yaml << 'YAML'
#   source:
#     type: duckdb
#     table: read_csv('orders.csv')
#   checks:
#     - not_null: [order_id, customer_id, amount]
#     - unique: order_id
#     - accepted_values:
#         column: status
#         values: [pending, shipped, delivered, cancelled]
#     - range:
#         column: amount
#         min: 0
#         max: 100000
#     - row_count:
#         min: 1
#   YAML
#
# Generate the GIF:
#   vhs docs/demo.tape -o docs/assets/demo.gif

# Terminal setup
Set Shell "bash"
Set Width 900
Set Height 500
Set FontSize 16
Set Theme "Dracula"
Set Padding 10
Set Framerate 30
Set TypingSpeed 40ms
Set CursorBlink false

# Show config file
Type "cat provero.yaml"
Sleep 500ms
Enter
Sleep 3s

# Run checks
Type "provero run"
Sleep 500ms
Enter
Sleep 5s

Sleep 2s
