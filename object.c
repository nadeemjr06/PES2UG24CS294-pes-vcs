cat > object.c << 'EOF'
// object.c — Content-addressable object store

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── TODO: Implemented ───────────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str;
    if      (type == OBJ_BLOB)   type_str = "blob";
    else if (type == OBJ_TREE)   type_str = "tree";
    else if (type == OBJ_COMMIT) type_str = "commit";
    else return -1;

    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    header_len += 1;

    size_t total_len = (size_t)header_len + len;
    uint8_t *full_object = malloc(total_len);
    if (!full_object) return -1;

    memcpy(full_object, header, header_len);
    memcpy(full_object + header_len, data, len);

    compute_hash(full_object, total_len, id_out);

    if (object_exists(id_out)) {
        free(full_object);
        return 0;
    }

    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);

    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755);

    char temp_path[512];
    snprintf(temp_path, sizeof(temp_path), "%s/%.2s/tmp_%s", OBJECTS_DIR, hex, hex + 2);

    int fd = open(temp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        free(full_object);
        return -1;
    }

    size_t written = 0;
    while (written < total_len) {
        ssize_t n = write(fd, full_object + written, total_len - written);
        if (n < 0) {
            close(fd);
            unlink(temp_path);
            free(full_object);
            return -1;
        }
        written += n;
    }
    free(full_object);

    if (fsync(fd) < 0) {
        close(fd);
        unlink(temp_path);
        return -1;
    }
    close(fd);

    char final_path[512];
    snprintf(final_path, sizeof(final_path), "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);

    if (rename(temp_path, final_path) < 0) {
        unlink(temp_path);
        return -1;
    }

    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

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

    ObjectID computed;
    compute_hash(raw, file_size, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(raw);
        return -1;
    }

    uint8_t *null_pos = memchr(raw, '\0', file_size);
    if (!null_pos) {
        free(raw);
        return -1;
    }

    if      (strncmp((char *)raw, "blob",   4) == 0) *type_out = OBJ_BLOB;
    else if (strncmp((char *)raw, "tree",   4) == 0) *type_out = OBJ_TREE;
    else if (strncmp((char *)raw, "commit", 6) == 0) *type_out = OBJ_COMMIT;
    else {
        free(raw);
        return -1;
    }

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
EOF
