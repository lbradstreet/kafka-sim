#include "kr_kafka.h"
#include <assert.h>
#include <string.h>

/* Linked against the actual shared library. Runs without a broker or Linux I/O. */
int main(void) {
    assert(kr_abi_version() == KR_ABI_VERSION);
    kr_producer_config config;
    memset(&config, 0xa5, sizeof(config));
    assert(kr_producer_config_init(&config, sizeof(config)) == KR_OK);
    assert(config.struct_size == sizeof(config));
    assert(kr_abi_version() == 4);
    assert(config.batch_target_mode == 0);
    assert(config.delivery_timeout_ns > config.request_timeout_ns);
    assert(config.max_live_leases > 0);
    assert(config.bootstrap_count == 0);
    assert(config.bootstrap == NULL);
    assert(config.username.ptr == NULL);
    assert(kr_producer_config_init(&config, sizeof(config) - 1) == KR_ERR_VERSION);

    /* Rejected before native startup; out always cleared on a failed create. */
    kr_producer *producer = (kr_producer *)&config;
    config.struct_size--;
    assert(kr_producer_create(&config, &producer) == KR_ERR_VERSION);
    assert(producer == NULL);
    config.struct_size++;
    config.max_live_leases = 0;
    assert(kr_producer_create(&config, &producer) == KR_ERR_INVALID);
    assert(producer == NULL);
    assert(kr_last_error(NULL) == KR_ERR_INVALID);
    kr_destroy(NULL);
    return 0;
}
