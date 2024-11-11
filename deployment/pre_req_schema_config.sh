#!/bin/bash

SCHEMA_DIR="../schema"
FILE_NAME="APP_SCHEMA.sql"

check_arguments() {
    if [ "$#" -ne 4 ]; then
        echo "Error: Exactly 4 parameters are required."
        exit 1
    fi

    for param in "$@"; do
        if [ -z "$param" ]; then
            echo "Error: No parameter should be empty."
            exit 1
        fi
    done
}

arg_db=$1
echo "MY_DEPLOY_DB: $arg_db"
arg_sch=$2
echo "MY_DEPLOY_SCHEMA: $arg_sch"
arg_stg=$3
echo "MY_DEPLOY_STAGE: $arg_stg"
arg_wh=$4
echo "MY_DEPLOY_WAREHOUSE: $arg_wh"

process_file() {
    local input_file="$SCHEMA_DIR/$FILE_NAME"
    local output_file="${input_file%.*}_modified.${input_file##*.}"
    echo "🔄 Processing: $input_file"
    local temp_file=$(mktemp)

    iconv -f ISO-8859-1 -t UTF-8 "$input_file" |
        sed -E "
        s/SNOW_DB/${arg_db}/gi
        s/SNOW_SCHEMA/${arg_sch}/gi
        s/SNOW_STAGE/${arg_stg}/gi
        s/SNOW_WAREHOUSE/${arg_wh}/gi   
    " |
        iconv -f UTF-8 -t UTF-8 >"$temp_file"

    if [ $? -eq 0 ]; then
        if ! cmp -s "$input_file" "$temp_file"; then
            mv -fv "$temp_file" "$output_file"
            echo "✅ Replaced: $output_file"
            return 0
        else
            echo "ℹ️  No changes needed in: $input_file"
            rm "$temp_file"
            return 1
        fi
    else
        echo "❌ Error processing: $input_file"
        rm "$temp_file"
        return 1
    fi
}

check_file() {
    for file in "$SCHEMA_DIR"/*; do
        if [[ -f "$file" ]]; then
            code_file=$(file -I "$file" | cut -d';' -f2)
            echo "$file: $code_file"
        fi
    done
}

replace_file() {
    tmp_file="$SCHEMA_DIR/APP_SCHEMA_modified.sql"
    target_file="$SCHEMA_DIR/APP_SCHEMA.sql"
    mv -f "$tmp_file" "$target_file"
    echo "✅ Success: Set connection arguments: $target_file"
}

get_encode_file() {
    input_file="$SCHEMA_DIR/APP_SCHEMA.sql"
    file -I "$input_file"
    echo "✅ Success: $input_file"
}

check_arguments "$@"
process_file
check_file
replace_file
get_encode_file
