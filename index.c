// index.c — Staging area implementation

#include "index.h"
#include "pes.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i],
                        &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;

    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;

    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec ||
                st.st_size  != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;

    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {

            if (strcmp(ent->d_name, ".") == 0 ||
                strcmp(ent->d_name, "..") == 0) continue;

            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;

            int tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    tracked = 1;
                    break;
                }
            }

            if (!tracked) {
                struct stat st;
                if (stat(ent->d_name, &st) == 0 && S_ISREG(st.st_mode)) {
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }

    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── IMPLEMENTED FUNCTIONS ───────────────────────────────────────────────────

// Load index
int index_load(Index *index) {
    index->count = 0;

    FILE *fp = fopen(".pes/index", "r");
    if (!fp) {
        return 0; // no index yet
    }

    char hash_hex[65];

    while (index->count < MAX_INDEX_ENTRIES) {
        IndexEntry *e = &index->entries[index->count];

        int ret = fscanf(fp, "%o %64s %lu %u %511[^\n]\n",
                         &e->mode,
                         hash_hex,
                         &e->mtime_sec,
                         &e->size,
                         e->path);

        if (ret == EOF) break;

        if (ret != 5) {
            fclose(fp);
            fprintf(stderr, "error: malformed index\n");
            return -1;
        }

        if (hex_to_hash(hash_hex, &e->hash) != 0) {
            fclose(fp);
            fprintf(stderr, "error: invalid hash\n");
            return -1;
        }

        index->count++;
    }

    fclose(fp);
    return 0;
}

// Sort helper
static int compare_entries(const void *a, const void *b) {
    return strcmp(((IndexEntry *)a)->path,
                  ((IndexEntry *)b)->path);
}

// Save index (atomic)
int index_save(const Index *index) {
    Index temp = *index;

    qsort(temp.entries, temp.count,
          sizeof(IndexEntry), compare_entries);

    FILE *fp = fopen(".pes/index.tmp", "w");
    if (!fp) {
        perror("fopen");
        return -1;
    }

    char hash_hex[65];

    for (int i = 0; i < temp.count; i++) {
        const IndexEntry *e = &temp.entries[i];

        hash_to_hex(&e->hash, hash_hex);

        fprintf(fp, "%o %s %lu %u %s\n",
                e->mode,
                hash_hex,
                e->mtime_sec,
                e->size,
                e->path);
    }

    fflush(fp);

    int fd = fileno(fp);
    if (fsync(fd) != 0) {
        perror("fsync");
        fclose(fp);
        return -1;
    }

    fclose(fp);

    if (rename(".pes/index.tmp", ".pes/index") != 0) {
        perror("rename");
        return -1;
    }

    return 0;
}

// Add (stage file)
int index_add(Index *index, const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        perror("stat");
        return -1;
    }

    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "error: '%s' is not a file\n", path);
        return -1;
    }

    FILE *fp = fopen(path, "rb");
    if (!fp) {
        perror("fopen");
        return -1;
    }

    void *buf = malloc(st.st_size);
    if (!buf) {
        fclose(fp);
        return -1;
    }

    if (fread(buf, 1, st.st_size, fp) != (size_t)st.st_size) {
        perror("fread");
        free(buf);
        fclose(fp);
        return -1;
    }

    fclose(fp);

    ObjectID hash;
    if (object_write(OBJ_BLOB, buf, st.st_size, &hash) != 0) {
        free(buf);
        return -1;
    }

    free(buf);

    IndexEntry *entry = index_find(index, path);

    if (!entry) {
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "error: index full\n");
            return -1;
        }
        entry = &index->entries[index->count++];
    }

    entry->mode = st.st_mode;
    entry->hash = hash;
    entry->mtime_sec = st.st_mtime;
    entry->size = st.st_size;

    strncpy(entry->path, path, sizeof(entry->path) - 1);
    entry->path[sizeof(entry->path) - 1] = '\0';

    return index_save(index);
}
