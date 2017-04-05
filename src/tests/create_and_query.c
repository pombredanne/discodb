#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <discodb.h>
#include <fcntl.h>
#include <ddb_internal.h>

#define MAX_KV_SIZE 1<<20

#define FEAT(x) (long long unsigned int)feat[x]

static const char yes[] = "true";
static const char no[] = "false";
const char *boolstr(int boolean) { return boolean ? yes: no; }
static void print_info(struct ddb *db)
{
    ddb_features_t feat;
    ddb_features(db, feat);
    printf("Total size:              %llu bytes\n", FEAT(DDB_TOTAL_SIZE));
    printf("Items size:              %llu bytes\n", FEAT(DDB_ITEMS_SIZE));
    printf("Values size:             %llu bytes\n", FEAT(DDB_VALUES_SIZE));
    printf("Number of keys:          %llu\n", FEAT(DDB_NUM_KEYS));
    printf("Number of items:         %llu\n", FEAT(DDB_NUM_VALUES));
    printf("Number of unique values: %llu\n", FEAT(DDB_NUM_UNIQUE_VALUES));
    printf("Compressed?              %s\n", boolstr(feat[DDB_IS_COMPRESSED]));
    printf("Hashed?                  %s\n", boolstr(feat[DDB_IS_HASHED]));
    printf("Multiset?                %s\n", boolstr(feat[DDB_IS_MULTISET]));
}
static struct ddb *open_discodb(const char *file)
{
    struct ddb *db;
    int fd;

    if (!(db = ddb_new())){
        fprintf(stderr, "Couldn't initialize discodb: Out of memory\n");
        exit(1);
    }
    if ((fd = open(file, O_RDONLY)) == -1){
        fprintf(stderr, "Couldn't open discodb %s\n", file);
        exit(1);
    }
    if (ddb_load(db, fd)){
        const char *err;
        ddb_error(db, &err);
        fprintf(stderr, "Invalid discodb in %s: %s\n", file, err);
        exit(1);
    }
    return db;
}

static void print_cursor(struct ddb *db, struct ddb_cursor *cur, struct ddb_entry *es)
{
    if (!cur){
        const char *err;
        ddb_error(db, &err);
        fprintf(stderr, "Query failed: %s\n", err);
        exit(1);
    }

    if (ddb_notfound(cur)){
        fprintf(stderr, "Not found\n");
        exit(1);
    }
    int errno, i = 0;
    const struct ddb_entry *e;
    while ((e = ddb_next(cur, &errno))){
        printf("Key: %.*s -- Value:%.*s\n", es->length, es->data, e->length, e->data);
        ++i;
    }
    if (errno){
        fprintf(stderr, "Cursor failed: out of memory\n");
        exit(1);
    }
    ddb_free_cursor(cur);
}

static void read_pairs(FILE *in, struct ddb_cons *db)
{
    char key[MAX_KV_SIZE];
    char val[MAX_KV_SIZE];
    uint32_t lc = 0;

    while(fscanf(in, "%s %s\n", key, val) == 2){
        struct ddb_entry key_e = {.data = key, .length = strlen(key)};
        struct ddb_entry val_e = {.data = val, .length = strlen(val)};
        if (ddb_cons_add(db, &key_e, &val_e)){
            fprintf(stderr, "Adding '%s':'%s' failed\n", key, val);
            exit(1);
        }
        ++lc;
    }
    fclose(in);
    fprintf(stderr, "%u key-value pairs read.\n", lc);
}

static void read_keys(FILE *in, struct ddb_cons *db)
{
    char key[MAX_KV_SIZE];
    uint32_t lc = 0;

    while(fscanf(in, "%s\n", key) == 1){
        struct ddb_entry key_e = {.data = key, .length = strlen(key)};
        if (ddb_cons_add(db, &key_e, NULL)){
            fprintf(stderr, "Adding '%s' failed\n", key);
            exit(1);
        }
        ++lc;
    }
    fclose(in);
    fprintf(stderr, "%u keys read.\n", lc);
}

int main(int argc, char **argv)
{
    if (argc < 2){
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "create_discodb discodb.out input.txt\n");
        fprintf(stderr, "where input.txt contain a key-value pair on each line, devided by space.\n");
        exit(1);
    }

    FILE *in;
    FILE *out;
    uint64_t size;
    char *data;
    struct ddb_cons *db = ddb_cons_new();
    uint64_t flags = 0;
    char * ddbfile = argv[1];
    char * kvfile  = argv[2];
    flags |= getenv("DONT_COMPRESS") ? DDB_OPT_DISABLE_COMPRESSION: 0;
    flags |= getenv("UNIQUE_ITEMS") ? DDB_OPT_UNIQUE_ITEMS: 0;
    flags |= DDB_OPT_UNIQUE_ITEMS;

    if (!db){
        fprintf(stderr, "DB init failed\n");
        exit(1);
    }

    if (!(in = fopen(kvfile, "r"))){
        fprintf(stderr, "Couldn't open %s\n", kvfile);
        exit(1);
    }
    if (getenv("KEYS_ONLY"))
        read_keys(in, db);
    else
        read_pairs(in, db);

    fprintf(stderr, "Packing the index..\n");

    if (!(data = ddb_cons_finalize(db, &size, flags))){
        fprintf(stderr, "Packing the index failed\n");
        exit(1);
    }
    struct ddb *sddb = ddb_new();
    ddb_loads(sddb, data, size);
    struct ddb_entry ee;
    ee.data = "key";
    ee.length = strlen(ee.data);
    print_cursor(sddb, ddb_getitem(sddb, &ee), &ee);
    ddb_free(sddb);

    ddb_cons_free(db);

    if (!(out = fopen(ddbfile, "w"))){
        fprintf(stderr, "Opening file %s failed\n", ddbfile);
        exit(1);
    }
    if (!fwrite(data, size, 1, out)){
        fprintf(stderr, "Writing file %s failed\n", ddbfile);
        exit(1);
    }
    fflush(out);
    fclose(out);
    free(data);
    fprintf(stderr, "Ok! Index written to %s\n", ddbfile);

    struct ddb *odb = open_discodb(ddbfile);
    print_info(odb);
    struct ddb_entry e;
    e.data = "key";
    e.length = strlen(e.data);
    print_cursor(odb, ddb_getitem(odb, &e), &e);



    ddb_free(odb);

    return 0;
}