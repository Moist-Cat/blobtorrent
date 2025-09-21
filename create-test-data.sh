#!/bin/bash
# create-test-data.sh - Script to create test data and torrent files for BitTorrent testing

set -e

# Configuration
OUTPUT_DIR="out"
TORRENT_DIR="torrent"
ORIGINAL_DIR="$OUTPUT_DIR/original"
FILES_DIR="$OUTPUT_DIR/files"
NUM_FILES=10
FILE_SIZE_KB=1024  # 1MB per file
TRACKER_URL="http://tracker:6969/announce"
#TRACKER_URL="http://localhost:6969/announce"
CREATOR="BlobTorrent-Test-Suite"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if mktorrent is installed
check_dependencies() {
    if ! command -v mktorrent &> /dev/null; then
        log_error "mktorrent is not installed. Please install it:"
        log_error "Ubuntu/Debian: sudo apt-get install mktorrent"
        log_error "macOS: brew install mktorrent"
        log_error "Or build from source: https://github.com/Rudde/mktorrent"
        exit 1
    fi
}

# Create directories
create_directories() {
    log_info "Creating directories..."
    mkdir -p "$ORIGINAL_DIR"
    mkdir -p "$FILES_DIR"
    mkdir -p "$TORRENT_DIR"
    
    # Create a subdirectory structure for more realistic testing
    mkdir -p "$ORIGINAL_DIR/subdir1"
    mkdir -p "$ORIGINAL_DIR/subdir2/nested"
    mkdir -p "$FILES_DIR/subdir1"
    mkdir -p "$FILES_DIR/subdir2/nested"
}

# Generate test files with different content patterns
generate_test_files() {
    log_info "Generating $NUM_FILES test files..."
    
    # Create files in root directory
    for i in $(seq 1 $NUM_FILES); do
        filename="file_$i.txt"
        echo "This is file $i - created at $(date)" > "$ORIGINAL_DIR/$filename"
        echo "File $i content: Lorem ipsum dolor sit amet, consectetur adipiscing elit." >> "$ORIGINAL_DIR/$filename"
        
        # Add some pattern to make files different
        for j in $(seq 1 100); do
            echo "Line $j: Pattern $(($i * $j))" >> "$ORIGINAL_DIR/$filename"
        done
        
        # Create a larger file by duplicating content
        cat "$ORIGINAL_DIR/$filename" "$ORIGINAL_DIR/$filename" > "$ORIGINAL_DIR/large_file_$i.txt"
    done
    
    # Create files in subdirectories
    for i in $(seq 1 3); do
        echo "Subdirectory file $i" > "$ORIGINAL_DIR/subdir1/subfile_$i.txt"
        echo "Nested file $i" > "$ORIGINAL_DIR/subdir2/nested/nested_file_$i.txt"
    done
    
    # Create a few binary files for variety
    head -c 1024 /dev/urandom > "$ORIGINAL_DIR/binary_data.bin"
    head -c 2048 /dev/urandom > "$ORIGINAL_DIR/subdir1/another_binary.bin"
    
    log_success "Generated test files in $ORIGINAL_DIR"
}

# Create different types of torrent files
create_torrents() {
    log_info "Creating torrent files..."
    
    # Single file torrent
    log_info "Creating single file torrent..."
    mktorrent -a "$TRACKER_URL" -c "$CREATOR" -n "single_file" -o "$TORRENT_DIR/single_file.torrent" "$ORIGINAL_DIR/file_1.txt"
    
    # Multiple files torrent (directory)
    log_info "Creating multi-file torrent..."
    mktorrent -a "$TRACKER_URL" -c "$CREATOR" -n "multi_file" -o "$TORRENT_DIR/multi_file.torrent" "$ORIGINAL_DIR"
    
    # Torrent with specific piece size
    log_info "Creating torrent with 256KB piece size..."
    mktorrent -a "$TRACKER_URL" -c "$CREATOR" -n "small_pieces" -l 18 -o "$TORRENT_DIR/small_pieces.torrent" "$ORIGINAL_DIR"
    
    # Torrent with larger piece size
    log_info "Creating torrent with 1MB piece size..."
    mktorrent -a "$TRACKER_URL" -c "$CREATOR" -n "large_pieces" -l 20 -o "$TORRENT_DIR/large_pieces.torrent" "$ORIGINAL_DIR"
    
    # Torrent without tracker (for DHT testing)
    log_info "Creating trackerless torrent..."
    mktorrent -c "$CREATOR" -n "trackerless" -o "$TORRENT_DIR/trackerless.torrent" "$ORIGINAL_DIR"
    
    # Create a torrent for each individual file
    log_info "Creating individual file torrents..."
    for file in "$ORIGINAL_DIR"/*.txt "$ORIGINAL_DIR"/*.bin; do
        if [ -f "$file" ]; then
            filename=$(basename "$file")
            torrent_name="${filename}"
            mktorrent -a "$TRACKER_URL" -c "$CREATOR" -n "$torrent_name" -o "$TORRENT_DIR/$torrent_name.torrent" "$file"
        fi
    done
    
    log_success "Created torrent files in $TORRENT_DIR"
}

# Generate file list and checksums for verification
generate_verification_data() {
    log_info "Generating verification data..."
    
    # Create file list with checksums
    find "$ORIGINAL_DIR" -type f -exec sha1sum {} \; > "$OUTPUT_DIR/checksums.sha1"
    
    # Create file list with sizes
    find "$ORIGINAL_DIR" -type f -exec du -b {} \; > "$OUTPUT_DIR/file_sizes.txt"
    
    # Create a manifest file
    cat > "$OUTPUT_DIR/manifest.json" << EOF
{
    "created": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "num_files": $(find "$ORIGINAL_DIR" -type f | wc -l),
    "total_size": $(du -sb "$ORIGINAL_DIR" | cut -f1),
    "file_types": {
        "txt": $(find "$ORIGINAL_DIR" -name "*.txt" | wc -l),
        "bin": $(find "$ORIGINAL_DIR" -name "*.bin" | wc -l)
    },
    "directory_structure": {
        "root": $(find "$ORIGINAL_DIR" -maxdepth 1 -type f | wc -l),
        "subdir1": $(find "$ORIGINAL_DIR/subdir1" -type f 2>/dev/null | wc -l || echo 0),
        "subdir2": $(find "$ORIGINAL_DIR/subdir2" -type f 2>/dev/null | wc -l || echo 0)
    }
}
EOF
    
    log_success "Generated verification data"
}

# Create a test script to verify downloads
create_verification_script() {
    cat > "$OUTPUT_DIR/verify_download.sh" << 'EOF'
#!/bin/bash
# verify_download.sh - Verify downloaded files against original checksums

set -e

DOWNLOAD_DIR="$1"
ORIGINAL_DIR="$2"
CHECKSUM_FILE="$3"

if [ $# -lt 3 ]; then
    echo "Usage: $0 <download_dir> <original_dir> <checksum_file>"
    exit 1
fi

echo "Verifying downloaded files..."

# Check if all files are present
missing_files=0
while read -r checksum filename; do
    rel_path=${filename#*/}
    if [ ! -f "$DOWNLOAD_DIR/$rel_path" ]; then
        echo "MISSING: $rel_path"
        missing_files=$((missing_files + 1))
    fi
done < "$CHECKSUM_FILE"

if [ $missing_files -gt 0 ]; then
    echo "Found $missing_files missing files"
    exit 1
fi

# Verify checksums
invalid_files=0
while read -r original_checksum filename; do
    rel_path=${filename#*/}
    downloaded_file="$DOWNLOAD_DIR/$rel_path"
    
    if [ -f "$downloaded_file" ]; then
        downloaded_checksum=$(sha1sum "$downloaded_file" | cut -d' ' -f1)
        if [ "$downloaded_checksum" != "$original_checksum" ]; then
            echo "INVALID: $rel_path (expected $original_checksum, got $downloaded_checksum)"
            invalid_files=$((invalid_files + 1))
        else
            echo "VALID: $rel_path"
        fi
    fi
done < "$CHECKSUM_FILE"

if [ $invalid_files -gt 0 ]; then
    echo "Found $invalid_files files with invalid checksums"
    exit 1
fi

echo "All files verified successfully!"
echo "Total files: $(find "$DOWNLOAD_DIR" -type f | wc -l)"
echo "Total size: $(du -sh "$DOWNLOAD_DIR" | cut -f1)"
EOF

    chmod +x "$OUTPUT_DIR/verify_download.sh"
}

# Main execution
main() {
    log_info "Starting test data creation..."
    log_info "Output directory: $OUTPUT_DIR"
    log_info "Torrent directory: $TORRENT_DIR"
    
    check_dependencies
    create_directories
    generate_test_files
    create_torrents
    generate_verification_data
    create_verification_script
    
    log_success "Test data creation completed!"
    log_success "Original files: $ORIGINAL_DIR"
    log_success "Torrent files: $TORRENT_DIR"
    log_success "Verification data: $OUTPUT_DIR/checksums.sha1"
    
    echo ""
    echo "=== Next Steps ==="
    echo "1. Copy torrent files to your torrent client: cp $TORRENT_DIR/*.torrent /path/to/torrents/"
    echo "2. Use the verification script: $OUTPUT_DIR/verify_download.sh <download_dir> $ORIGINAL_DIR $OUTPUT_DIR/checksums.sha1"
    echo ""
    echo "=== Available Torrents ==="
    ls -la "$TORRENT_DIR"/*.torrent
}

# Run main function
main "$@"
