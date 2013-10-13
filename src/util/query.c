
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <discodb.h>

static void print_cursor(struct ddb *db, struct ddb_cursor *cur)
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
                printf("%.*s\n", e->length, e->data);
                ++i;
        }
        if (errno){
            fprintf(stderr, "Cursor failed: out of memory\n");
            exit(1);
        }
        ddb_free_cursor(cur);
}

static struct ddb_query_clause *parse_cnf(char **tokens, int num, int *num_clauses)
{
        int k, i, j, t;
        *num_clauses = 1;
        for (i = 0; i < num; i++)
                if (!strcmp(tokens[i], "&"))
                        ++*num_clauses;

        struct ddb_query_term *terms = calloc(num - (*num_clauses - 1),
                sizeof(struct ddb_query_term));
        struct ddb_query_clause *clauses = calloc(*num_clauses,
                sizeof(struct ddb_query_clause));

        clauses[0].terms = terms;

        for (t = 0, k = 0, j = 0, i = 0; i < num; i++){
                if (!strcmp(tokens[i], "&")){
                        clauses[j++].num_terms = i - k;
                        clauses[j].terms = &terms[t];
                        k = t;
                        continue;
                }
                if (tokens[i][0] == '~'){
                        terms[t].nnot = 1;
                        terms[t].key.data = &tokens[i][1];
                        terms[t].key.length = strlen(&tokens[i][1]);
                }else{
                        terms[t].key.data = tokens[i];
                        terms[t].key.length = strlen(tokens[i]);
                }
                ++t;
        }
        clauses[j].num_terms = t - k;
#ifdef DEBUG
        for (i = 0; i < *num_clauses; i++){
                printf("dbg Clause:\n");
                for (j = 0; j < clauses[i].num_terms; j++){
                        if (clauses[i].terms[j].nnot)
                                printf("dbg NOT ");
                        printf("dbg %.*s\n", clauses[i].terms[j].key.length,
                                clauses[i].terms[j].key.data);
                }
                printf("dbg ---\n");
        }
#endif
        return clauses;
}

static struct ddb_view *load_view(const char *file, struct ddb *db)
{
    int fd, i, n;
    struct stat nfo;
    char *p;
    struct ddb_view *view;
    struct ddb_view_cons *cons;

    if (!(cons = ddb_view_cons_new()))
        return NULL;

    if ((fd = open(file, O_RDONLY)) == -1)
        return NULL;

    if (fstat(fd, &nfo))
        return NULL;

    if (!(p = mmap(0, nfo.st_size, PROT_READ, MAP_SHARED, fd, 0)))
        goto err;

    close(fd);

    i = n = 0;
    while (i < nfo.st_size){
        struct ddb_entry e;
        e.data = &p[i];
        e.length = 0;
        while (i < nfo.st_size && p[i] != 10){
            ++e.length;
            ++i;
        }
        ++i;
        if (ddb_view_cons_add(cons, &e))
            goto err;
    }
    view = ddb_view_cons_finalize(cons, db);
    ddb_view_cons_free(cons);
    munmap(p, nfo.st_size);
    return view;
err:
    ddb_view_cons_free(cons);
    if (p)
        munmap(p, nfo.st_size);
    return NULL;
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

static void usage()
{
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "query_discodb [discodb] [-keys|-values|-uvalues|-info|-item|-cnf] [query]\n");
        fprintf(stderr, "cnf format example: a b & ~c d & e\n");
        exit(1);
}

int main(int argc, char **argv)
{
        if (argc < 3)
                usage();

        struct ddb *db = open_discodb(argv[1]);
        if (!strcmp(argv[2], "-info"))
                print_info(db);
        else if (!strcmp(argv[2], "-keys"))
                print_cursor(db, ddb_keys(db));

        else if (!strcmp(argv[2], "-values"))
                print_cursor(db, ddb_values(db));

        else if (!strcmp(argv[2], "-uvalues"))
                print_cursor(db, ddb_unique_values(db));

        else if (!strcmp(argv[2], "-item")){
                if (argc < 4){
                        fprintf(stderr, "Specify query\n");
                        exit(1);
                }
                struct ddb_entry e;
                e.data = argv[3];
                e.length = strlen(argv[3]);
                print_cursor(db, ddb_getitem(db, &e));
        }else if (!strcmp(argv[2], "-cnf")){
                if (argc < 4){
                        fprintf(stderr, "Specify query\n");
                        exit(1);
                }
                int num_q = 0;
                struct ddb_query_clause *q = parse_cnf(&argv[3], argc - 3, &num_q);
                char *view_file = getenv("VIEW");
                struct ddb_view *view = NULL;
                if (view_file){
                    if ((view = load_view(view_file, db))){
                        fprintf(stderr, "View loaded successfully (%d items)\n",
                                ddb_view_size(view));
                    }else{
                        fprintf(stderr, "Loading view from %s failed\n", view_file);
                        exit(1);
                    }
                }
                print_cursor(db, ddb_query_view(db, q, num_q, view));
                free(q[0].terms);
                free(q);
                ddb_view_free(view);
        }else
                usage();

        ddb_free(db);
        return 0;
}
