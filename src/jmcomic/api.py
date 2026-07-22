import asyncio
import inspect

from .jm_downloader import *

__DOWNLOAD_API_RET = DownloadResult


async def _invoke_async_callback(callback, entity, downloader):
    if callback is None:
        return None

    is_async_callback = (
            inspect.iscoroutinefunction(callback)
            or inspect.iscoroutinefunction(getattr(callback, '__call__', None))
    )
    if is_async_callback:
        return await callback(entity, downloader)

    result = await asyncio.to_thread(callback, entity, downloader)
    if inspect.isawaitable(result):
        return await result
    return result


def download_batch(download_api,
                   jm_id_iter: Union[Iterable, Generator],
                   option=None,
                   downloader=None,
                   **kwargs,
                   ) -> BatchResult:
    """
    批量下载 album / photo

    一个album/photo，对应一个线程，对应一个option。
    返回 BatchResult(set)，支持 for album, dler in result 遍历。
    失败项收集在 result.failed 中，不会静默丢失。

    :param download_api: 下载api
    :param jm_id_iter: jmid (album_id, photo_id) 的迭代器
    :param option: 下载选项，所有的jmid共用一个option
    :param downloader: 下载器类
    """
    from common import multi_thread_launcher

    if option is None:
        option = JmModuleConfig.option_class().default()

    result = BatchResult()

    def _safe_download(aid):
        """batch 内部的单任务包装：确保异常被收集而非静默丢失"""
        try:
            ret = download_api(aid, option, downloader, **kwargs)
            result.add(ret)
        except Exception as e:
            jm_log('batch.failed', f'批量下载失败: [{aid}], 异常: [{e}]', e)
            result.failed[str(aid)] = e

    multi_thread_launcher(
        iter_objs=set(
            JmcomicText.parse_to_jm_id(jmid)
            for jmid in jm_id_iter
        ),
        apply_each_obj_func=_safe_download,
        wait_finish=True
    )

    return result


def download_album(jm_album_id,
                   option=None,
                   downloader=None,
                   callback=None,
                   check_exception=True,
                   extra=None,
                   ) -> Union[__DOWNLOAD_API_RET, Set[__DOWNLOAD_API_RET]]:
    """
    下载一个本子（album），包含其所有的章节（photo）
    当jm_album_id不是str或int时，视为批量下载
    :param jm_album_id: 本子的禁漫车号
    :param option: 下载选项
    :param downloader: 下载器类
    :param callback: 返回值回调函数，可以拿到 album 和 downloader
    :param check_exception: 是否检查异常
    :param extra: 下载特性（Feature）
    :return: DownloadResult (如果是批量情况返回 BatchResult)
    """
    if not isinstance(jm_album_id, (str, int)):
        return download_batch(download_album, jm_album_id, option, downloader, extra=extra)

    return _download_and_return(
        jm_album_id, option, downloader, callback, check_exception, extra,
        'download_album', lambda d: d.download_album(jm_album_id),
    )


def download_photo(jm_photo_id,
                   option=None,
                   downloader=None,
                   callback=None,
                   check_exception=True,
                   extra=None,
                   ):
    """
    下载一个章节（photo）
    当jm_photo_id不是str或int时，视为批量下载
    :param jm_photo_id: 章节的禁漫车号
    :param option: 下载选项
    :param downloader: 下载器类
    :param callback: 返回值回调函数
    :param check_exception: 是否检查异常
    :param extra: 下载特性（Feature）
    """
    if not isinstance(jm_photo_id, (str, int)):
        return download_batch(download_photo, jm_photo_id, option, downloader, extra=extra)

    return _download_and_return(
        jm_photo_id, option, downloader, callback, check_exception, extra,
        'download_photo', lambda d: d.download_photo(jm_photo_id),
    )


def _download_and_return(jm_id, option, downloader, callback, check_exception, extra,
                         feature_source, download_fn):
    with new_downloader(option, downloader) as dler:
        dler.add_features(extra, feature_source)
        entity = download_fn(dler)

        if callback is not None:
            callback(entity, dler)
        if check_exception:
            dler.raise_if_has_exception()
        return DownloadResult(entity, dler)


def new_downloader(option=None, downloader=None) -> JmDownloader:
    if option is None:
        option = JmModuleConfig.option_class().default()

    if downloader is None:
        downloader = JmModuleConfig.downloader_class()

    return downloader(option)


def create_option_by_file(filepath):
    return JmModuleConfig.option_class().from_file(filepath)


def create_option_by_env(env_name='JM_OPTION_PATH'):
    from .cli import get_env

    filepath = get_env(env_name, None)
    ExceptionTool.require_true(filepath is not None,
                               f'未配置环境变量: {env_name}，请配置为option的文件路径')
    return create_option_by_file(filepath)


def create_option_by_str(text: str, mode=None):
    if mode is None:
        mode = PackerUtil.mode_yml
    data = PackerUtil.unpack_by_str(text, mode)[0]
    return JmModuleConfig.option_class().construct(data)


create_option = create_option_by_file


def new_async_downloader(option=None, downloader=None):
    from .jm_async_downloader import JmAsyncDownloader
    if option is None:
        option = JmModuleConfig.option_class().default()

    if downloader is None:
        downloader = JmAsyncDownloader

    return downloader(option)


async def download_album_async(jm_album_id,
                               option=None,
                               downloader=None,
                               callback=None,
                               check_exception=True,
                               extra=None,
                               ):
    """
    异步下载一个本子（album），包含其所有的章节（photo）
    callback 支持同步函数和异步函数
    返回的 downloader 已关闭网络和线程池资源，仅用于读取下载结果
    """
    if not isinstance(jm_album_id, (str, int)):
        return await download_batch_async(download_album_async,
                                          jm_album_id,
                                          option,
                                          downloader,
                                          extra=extra
                                          )

    return await _download_async_and_return(
        jm_album_id, option, downloader, callback, check_exception, extra,
        'download_album', lambda d: d.download_album(jm_album_id),
    )


async def download_photo_async(jm_photo_id,
                               option=None,
                               downloader=None,
                               callback=None,
                               check_exception=True,
                               extra=None,
                               ):
    """
    异步下载一个章节（photo）
    callback 支持同步函数和异步函数
    返回的 downloader 已关闭网络和线程池资源，仅用于读取下载结果
    """
    if not isinstance(jm_photo_id, (str, int)):
        return await download_batch_async(download_photo_async,
                                          jm_photo_id,
                                          option,
                                          downloader,
                                          extra=extra
                                          )

    return await _download_async_and_return(
        jm_photo_id, option, downloader, callback, check_exception, extra,
        'download_photo', lambda d: d.download_photo(jm_photo_id),
    )


async def _download_async_and_return(jm_id, option, downloader, callback, check_exception, extra,
                                     feature_source, download_fn):
    async with new_async_downloader(option, downloader) as dler:
        dler.add_features(extra, feature_source)
        entity = await download_fn(dler)

        await _invoke_async_callback(callback, entity, dler)
        if check_exception:
            dler.raise_if_has_exception()

        return DownloadResult(entity, dler)


async def download_batch_async(download_api,
                               jm_id_iter,
                               option=None,
                               downloader=None,
                               **kwargs,
                               ) -> BatchResult:
    """
    异步批量下载 album / photo。
    - 容错机制：单个 album/photo 失败不会中止整批，也不会丢失其它已完成结果。
    - 返回 BatchResult(set)，失败项收集在 result.failed 中。
    """
    if option is None:
        option = JmModuleConfig.option_class().default()

    jm_ids = list(dict.fromkeys(JmcomicText.parse_to_jm_id(jmid) for jmid in jm_id_iter))

    results = await asyncio.gather(
        *(download_api(jmid, option, downloader, **kwargs) for jmid in jm_ids),
        return_exceptions=True,
    )

    # 失败不抛出，但要记录到 result.failed，便于调用者排查
    result = BatchResult()
    for jmid, r in zip(jm_ids, results):
        if isinstance(r, BaseException):
            jm_log('async.batch.failed', f'批量下载失败: [{jmid}], 异常: [{r}]', r)
            result.failed[str(jmid)] = r
        else:
            result.add(r)

    return result
