#!/bin/bash

echo "🔄 No-Code ETL Studio Setup and Run Script"
echo "=========================================="

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

# Check if pip is installed
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 is not installed. Please install pip."
    exit 1
fi

echo "📋 Python version:"
python3 --version

echo ""
echo "📦 Installing dependencies..."
pip3 install -r requirements.txt

echo ""
echo "✅ Dependencies installed successfully!"
echo ""
echo "🚀 Starting ETL Studio..."
echo ""
echo "📊 Your application will open in a web browser at:"
echo "   http://localhost:8501"
echo ""
echo "📝 To stop the application, press Ctrl+C in this terminal"
echo ""

# Run the Streamlit application
streamlit run app.py --server.port=8501 --server.address=localhost