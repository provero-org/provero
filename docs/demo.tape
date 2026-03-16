# Provero Demo - Terminal Recording
#
# This VHS tape file generates a ~15-second demo GIF showing
# provero init and provero run with colorful table output.
#
# Requirements:
#   - charmbracelet/vhs: go install github.com/charmbracelet/vhs@latest
#   - provero must be installed and available in PATH
#
# Generate the GIF:
#   vhs docs/demo.tape -o docs/assets/demo.gif

# Terminal setup
Set Shell "bash"
Set Width 80
Set Height 24
Set FontSize 14
Set Theme "Dracula"
Set Padding 20
Set Framerate 30

# Start with a clean terminal
Type "# Provero: declarative data quality engine"
Enter
Sleep 1s

# Initialize a new provero project
Type "provero init"
Sleep 500ms
Enter
Sleep 2s

# Show the generated YAML file (briefly)
Type "cat provero.yaml"
Sleep 500ms
Enter
Sleep 3s

# Run the quality checks
Type "provero run"
Sleep 500ms
Enter
Sleep 5s

# Pause on the results table
Sleep 3s
