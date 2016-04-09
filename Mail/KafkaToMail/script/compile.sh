#!/usr/bin/env bash

cd ../../../../../../../
mvn compile
mvn clean package

#mvn dependency:tree    显示应用所有的依赖，包括传递性依赖,  当有相同包的版本冲突时可利用exclusions进行排除指定包中的依赖
#mvn dependency:analyze 分析应用的依赖是否有问题


#下面的方法可以使用mvn dependency:tree进行查看
#当两个依赖路径上有两个版本的依赖X时，有以下两个依赖调解原则：
#第一原则：路径最近者优先；
#第二原则：路径长度一样时，第一声明者优先。
