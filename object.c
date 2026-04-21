int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // Step 1: Build the header string e.g. "blob 16\0"
    const char *type_str;
    if      (type == OBJ_BLOB)   type_str = "blob";
    else if (type == OBJ_TREE)   type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    header_len += 1; // include the '\0' terminator in the header

    // Step 2: Combine header + data into one buffer
    size_t total_len = header_len + len;
    uint8_t *full_object = malloc(total_len);
    if (!full_object) return -1;

    memcpy(full_object, header, header_len);       // copy "blob 16\0"
    memcpy(full_object + header_len, data, len);   // copy actual data

    // Step 3: Compute SHA-256 of the full object
    compute_hash(full_object, total_len, id_out);

    // Step 4: Deduplication — if it already exists, we're done
    if (object_exists(id_out)) {
        free(full_object);
        return 0;
    }

    // Step 5: Build the shard dir path e.g. ".pes/objects/2f/"
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);

    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755); // create shard dir (ignore error if already exists)

    // Step 6: Build final path and temp path
    char final_path[512];
    snprintf(final_path, sizeof(final_path), "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);

    char temp_path[512];
    snprintf(temp_path, sizeof(temp_path), "%s/%.2s/tmp_XXXXXX", OBJECTS_DIR, hex);

    // Step 7: Write to temp file first (atomic write pattern)
    int fd = mkstemp(temp_path); // creates a unique temp file
    if (fd < 0) {
        free(full_object);
        return -1;
    }

    ssize_t written = write(fd, full_object, total_len);
    free(full_object);

    if (written != (ssize_t)total_len) {
        close(fd);
        unlink(temp_path);
        return -1;
    }

    // Step 8: fsync the temp file — ensure data is on disk before rename
    if (fsync(fd) < 0) {
        close(fd);
        unlink(temp_path);
        return -1;
    }
    close(fd);

    // Step 9: Atomically rename temp -> final path
    if (rename(temp_path, final_path) < 0) {
        unlink(temp_path);
        return -1;
    }

    // Step 10: fsync the shard directory to persist the rename
    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    return 0;
}


int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // Step 1: Get the file path from the hash
    char path[512];
    object_path(id, path, sizeof(path));

    // Step 2: Open and read the entire file into memory
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (file_size <= 0) {
        fclose(f);
        return -1;
    }

    uint8_t *raw = malloc(file_size);
    if (!raw) {
        fclose(f);
        return -1;
    }

    if (fread(raw, 1, file_size, f) != (size_t)file_size) {
        fclose(f);
        free(raw);
        return -1;
    }
    fclose(f);

    // Step 3: Integrity check — re-hash the file contents
    ObjectID computed;
    compute_hash(raw, file_size, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(raw);
        return -1; // corrupted object
    }

    // Step 4: Parse the header — find the '\0' separating header from data
    uint8_t *null_pos = memchr(raw, '\0', file_size);
    if (!null_pos) {
        free(raw);
        return -1;
    }

    // Step 5: Parse type string ("blob", "tree", or "commit")
    if      (strncmp((char *)raw, "blob",   4) == 0) *type_out = OBJ_BLOB;
    else if (strncmp((char *)raw, "tree",   4) == 0) *type_out = OBJ_TREE;
    else if (strncmp((char *)raw, "commit", 6) == 0) *type_out = OBJ_COMMIT;
    else {
        free(raw);
        return -1;
    }

    // Step 6: Extract the data portion (everything after the '\0')
    uint8_t *data_start = null_pos + 1;
    size_t data_len = file_size - (data_start - raw);

    uint8_t *out = malloc(data_len);
    if (!out) {
        free(raw);
        return -1;
    }

    memcpy(out, data_start, data_len);
    *data_out = out;
    *len_out  = data_len;

    free(raw);
    return 0;
}
