#ifndef CCPOOL_FUSEKIT_BRIDGE_H
#define CCPOOL_FUSEKIT_BRIDGE_H

#include <stdint.h>

int32_t CCPoolFuseKitDispatchChild(void);
int32_t CCPoolFuseKitStart(const char *app_group_identifier);
int32_t CCPoolFuseKitReady(void);
int32_t CCPoolFuseKitWait(void);
int32_t CCPoolFuseKitStop(void);

#endif
