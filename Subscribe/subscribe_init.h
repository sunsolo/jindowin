//  Copyright (c) 2015-2015 The subscribe Authors. All rights reserved.
//  Created on: 2015/12/16 Author: Sunsolo

#ifndef __SUBSCRIBE__SUBSCRIBE__SUBSCRIBE_INIT___
#define __SUBSCRIBE__SUBSCRIBE__SUBSCRIBE_INIT___

#ifdef __cplusplus
extern "C" {
#endif

    /* ----------------------------------------------------------------------*/
    /**
     * @Synopsis  提供给core.so的接口，命名有一定的规则，就是插件名+_plugin_init
     *
     * @Param pl  要初始化的结构体，里面保存了指向固定接口函数的指针(逻辑函数)
     *
     * @Returns   
     */
    /* ----------------------------------------------------------------------*/
    int subscribe_plugin_init(struct plugin *pl);
#ifdef __cplusplus
}
#endif

#endif
