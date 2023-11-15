
> 注：当前项目为 Serverless Devs 应用，由于应用中会存在需要初始化才可运行的变量（例如应用部署地区、函数名等等），所以**不推荐**直接 Clone 本仓库到本地进行部署或直接复制 s.yaml 使用，**强烈推荐**通过 `s init --project ${模版名称}` 的方法或应用中心进行初始化，详情可参考[部署 & 体验](#部署--体验) 。

# start-zip-oss-v3 帮助文档
<p align="center" class="flex justify-center">
    <a href="https://www.serverless-devs.com" class="ml-1">
    <img src="http://editor.devsapp.cn/icon?package=start-zip-oss-v3&type=packageType">
  </a>
  <a href="http://www.devsapp.cn/details.html?name=start-zip-oss-v3" class="ml-1">
    <img src="http://editor.devsapp.cn/icon?package=start-zip-oss-v3&type=packageVersion">
  </a>
  <a href="http://www.devsapp.cn/details.html?name=start-zip-oss-v3" class="ml-1">
    <img src="http://editor.devsapp.cn/icon?package=start-zip-oss-v3&type=packageDownload">
  </a>
</p>

<description>

使用函数计算zip打包下载OSS文件

</description>

<codeUrl>

- [:smiley_cat: 代码](https://github.com/devsapp/start-zip-oss/tree/V3/src)

</codeUrl>
<preview>



</preview>


## 前期准备

使用该项目，您需要有开通以下服务：

<service>



| 服务 |  备注  |
| --- |  --- |
| 函数计算 FC |  需要创建函数处理核心业务逻辑 |
| 对象存储 OSS |  需要拉取存储 OSS 文件 |

</service>

推荐您拥有以下的产品权限 / 策略：
<auth>



| 服务/业务 |  权限 |  备注  |
| --- |  --- |   --- |
| 函数计算 | AliyunFCFullAccess |  需要创建函数处理核心业务逻辑 |
| 对象存储 | AliyunOSSFullAccess |  需要拉取存储 OSS 文件 |

</auth>

<remark>

您还需要注意：   
您还需要注意：  
OSS 创建的 bucket 和 应用函数需在同一个 region

</remark>

<disclaimers>

免责声明：   
免责声明：  
本项目打包使用标准的 python zipfile lib 处理，因需要打包的文件大小、数量不同函数执行时间不同，需根据情况具体测试打包时间以及所产生的费用。

</disclaimers>

## 部署 & 体验

<appcenter>
   
- :fire: 通过 [Serverless 应用中心](https://fcnext.console.aliyun.com/applications/create?template=start-zip-oss-v3) ，
  [![Deploy with Severless Devs](https://img.alicdn.com/imgextra/i1/O1CN01w5RFbX1v45s8TIXPz_!!6000000006118-55-tps-95-28.svg)](https://fcnext.console.aliyun.com/applications/create?template=start-zip-oss-v3) 该应用。
   
</appcenter>
<deploy>
    
- 通过 [Serverless Devs Cli](https://www.serverless-devs.com/serverless-devs/install) 进行部署：
  - [安装 Serverless Devs Cli 开发者工具](https://www.serverless-devs.com/serverless-devs/install) ，并进行[授权信息配置](https://docs.serverless-devs.com/fc/config) ；
  - 初始化项目：`s init --project start-zip-oss-v3 -d start-zip-oss-v3`
  - 进入项目，并进行项目部署：`cd start-zip-oss-v3 && s deploy -y`
   
</deploy>

## 应用详情

<appdetail id="flushContent">

打包下载 OSS 上存储的多个文件，例如将 OSS 上的一个目录打包下载。这样可以节省网络传输的数据，达到减少费用和下载时间的效果。使用 Serverless Devs 开发者工具，您只需要几步，就可以体验 Serverless 架构，带来的降本提效的技术红利。

</appdetail>

## 使用文档

<usedetail id="flushContent">

## 调用函数

应用部署成功后， 会输出 HTTP trigger 对应的公网访问地址
![](https://img.alicdn.com/imgextra/i3/O1CN013J7B3G1E2Eny1yjR1_!!6000000000293-2-tps-908-144.png)

1. 在 OSS 上准备要打包的文件， OSS 和 应用函数在同一个 region
   - 把文件放在 OSS 上面一个目录下面， 比如 `files`目录

> Tips: 除了支持 `source-dir` 指定目录， 也可以使用 `"source-files" :['1.txt', 'a/2.txt']` 这种形式指定多个文件

2. 触发函数(通过 HTTP trigger 对应的公网访问地址)
   - 使用 curl 命令直接调用函数

```bash
cat <<EOF > event.json
{
  "bucket": "fc-test-bucket",
  "source-dir": "files/"
}
EOF

curl -v -L -o /tmp/my.zip -d @./event.json https://zip-oss-func-zip-oss-xxxx.cn-shanghai.fcapp.run
```

打开`/tmp/my.zip`，就是`files/`目录下所有文件的压缩包。

> **注意**, 如果您有需求将上面的示例中的匿名非鉴权的 HTTP 函数改成需要鉴权的 HTTP 函数，可以采用相关的 sdk 去调用
>
> - [python sdk](https://github.com/aliyun/fc-python-sdk/blob/master/fc2/client.py#L125)
> - [java sdk](https://github.com/aliyun/fc-java-sdk/blob/bef94ddecad395503bb49476e3886a86e7dd9bcf/src/test/java/com/aliyuncs/fc/FunctionComputeClientTest.java#L2165)
> - [nodejs sdk](https://github.com/aliyun/fc-nodejs-sdk/blob/master/lib/client.js#L103)

### 方案

使用函数计算先把多个文件压缩成一个 zip，存储到 OSS 上面，返回 zip 文件的地址，客户端下载此文件。一般的客户端都支持跟随 HTTP 302 跳转地址，所以在完成压缩后，返回一个 302 的地址，客户端再跟随这个地址下载压缩后的文件包。

![zip_oss_high](https://img.alicdn.com/tfs/TB1GitkyeL2gK0jSZPhXXahvXXa-1258-946.png)

## 实现细节

1. 函数运行环境的磁盘空间是有限的，采用流式下载和上传的方式，只在内存中缓存少量的数据
2. 为了加快速度，一边生成 zip 文件时一边上传到 OSS
3. 上传 zip 文件到 OSS 时，利用 OSS 分片上传的特性，多线程并发上传

![zip_oss_low](https://img.alicdn.com/tfs/TB13jVqyoY1gK0jSZFCXXcwqXXa-774-1066.png)

## 实验数据

| #   | 文件数 | 压缩前总大小 | 压缩后总大小 | 执行时间 |
| --- | ------ | ------------ | ------------ | -------- |
| 1   | 7      | 1.2MB        | 1.16MB       | 0.4s     |
| 2   | 57     | 1.06GB       | 0.91GB       | 63s      |

通过 Serverless Devs 开发者工具，您只需要几步，就可以体验 Serverless 架构，带来的降本提效的技术红利。

</usedetail>


<devgroup>


## 开发者社区

您如果有关于错误的反馈或者未来的期待，您可以在 [Serverless Devs repo Issues](https://github.com/serverless-devs/serverless-devs/issues) 中进行反馈和交流。如果您想要加入我们的讨论组或者了解 FC 组件的最新动态，您可以通过以下渠道进行：

<p align="center">  

| <img src="https://serverless-article-picture.oss-cn-hangzhou.aliyuncs.com/1635407298906_20211028074819117230.png" width="130px" > | <img src="https://serverless-article-picture.oss-cn-hangzhou.aliyuncs.com/1635407044136_20211028074404326599.png" width="130px" > | <img src="https://serverless-article-picture.oss-cn-hangzhou.aliyuncs.com/1635407252200_20211028074732517533.png" width="130px" > |
| --------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| <center>微信公众号：`serverless`</center>                                                                                         | <center>微信小助手：`xiaojiangwh`</center>                                                                                        | <center>钉钉交流群：`33947367`</center>                                                                                           |
</p>
</devgroup>
