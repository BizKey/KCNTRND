#!/usr/bin/env python
"""KCN2 trading bot for kucoin."""

import asyncio
from base64 import b64encode
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from hashlib import sha256
from hmac import HMAC
from hmac import new as hmac_new
from os import environ
from time import time
from typing import Any, Self
from urllib.parse import urljoin
from uuid import UUID, uuid4

from aiohttp import ClientConnectorError, ClientSession, ServerDisconnectedError
from asyncpg import Pool, Record, create_pool
from dacite import (
    ForwardReferenceError,
    MissingValueError,
    StrictUnionMatchError,
    UnexpectedDataError,
    UnionMatchError,
    WrongTypeError,
    from_dict,
)
from loguru import logger
from orjson import JSONDecodeError, JSONEncodeError, dumps, loads
from result import Err, Ok, Result, do, do_async


@dataclass
class AlertestToken:
    """."""

    all_tokens: list[str]
    deleted_tokens: list[str]
    new_tokens: list[str]


@dataclass
class Book:
    """Store data for each token."""

    baseincrement: Decimal
    priceincrement: Decimal


@dataclass(frozen=True)
class TelegramSendMsg:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        ok: bool


@dataclass(frozen=True)
class ApiV2SymbolsGET:
    """https://www.kucoin.com/docs/rest/spot-trading/market-data/get-symbols-list."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            baseCurrency: str
            quoteCurrency: str
            baseIncrement: str
            priceIncrement: str
            isMarginEnabled: bool

        data: list[Data]
        code: str
        msg: str | None


@dataclass(frozen=True)
class ApiV1MarketCandleGET:
    """https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-klines."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        data: list[list[str]]
        code: str
        msg: str | None


@dataclass(frozen=True)
class ApiV1StopOrderOrderIdDELETE:
    """https://www.kucoin.com/docs-new/rest/spot-trading/orders/cancel-stop-order-by-orderld."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        code: str
        msg: str | None


@dataclass(frozen=True)
class ApiV1StopOrderGET:
    """https://www.kucoin.com/docs-new/rest/spot-trading/orders/get-stop-orders-list."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            @dataclass(frozen=True)
            class Item:
                """."""

                id: str

            items: list[Item]

        data: Data
        code: str
        msg: str | None


@dataclass(frozen=True)
class ApiV1StopOrderPOST:
    """https://www.kucoin.com/docs-new/rest/spot-trading/orders/add-stop-order."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            orderId: str

        data: Data
        code: str
        msg: str | None


@dataclass(frozen=True)
class ApiV3MarginAccountsGET:
    """https://www.kucoin.com/docs/rest/funding/funding-overview/get-account-detail-cross-margin."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            @dataclass(frozen=True)
            class Account:
                """."""

                currency: str
                liability: str
                available: str

            accounts: list[Account]
            debtRatio: str

        data: Data
        code: str
        msg: str | None


@dataclass(frozen=True)
class ApiV1AccountsGET:
    """https://www.kucoin.com/docs-new/rest/account-info/account-funding/get-account-list-spot."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            id: str
            currency: str
            type: str
            balance: str
            available: str
            holds: str

        data: list[Data]
        code: str
        msg: str | None


@dataclass(frozen=True)
class OrderChangeV2:
    """."""

    @dataclass(frozen=True)
    class Res:
        """Parse response request."""

        @dataclass(frozen=True)
        class Data:
            """."""

            symbol: str  # BTC-USDT
            side: str
            orderType: str
            type: str
            price: str | None  # fix for market order type
            size: str | None
            matchSize: str | None
            matchPrice: str | None

        data: Data


class KCN:
    """Main class collect all logic."""

    def init_envs(self: Self) -> Result[None, Exception]:
        """Init settings."""
        # All about excange
        self.KEY = self.get_env("KEY").unwrap()
        self.SECRET = self.get_env("SECRET").unwrap()
        self.PASSPHRASE = self.get_env("PASSPHRASE").unwrap()
        self.BASE_URL = self.get_env("BASE_URL").unwrap()

        # all about tokens
        self.ALL_CURRENCY = self.get_list_env("ALLCURRENCY").unwrap()
        self.IGNORECURRENCY = self.get_list_env("IGNORECURRENCY").unwrap()
        self.BASE_KEEP = Decimal(self.get_env("BASE_KEEP").unwrap())

        # All about tlg
        self.TELEGRAM_BOT_API_KEY = self.get_env("TELEGRAM_BOT_API_KEY").unwrap()
        self.TELEGRAM_BOT_CHAT_ID = self.get_list_env("TELEGRAM_BOT_CHAT_ID").unwrap()

        # db store
        self.PG_USER = self.get_env("PG_USER").unwrap()
        self.PG_PASSWORD = self.get_env("PG_PASSWORD").unwrap()
        self.PG_DATABASE = self.get_env("PG_DATABASE").unwrap()
        self.PG_HOST = self.get_env("PG_HOST").unwrap()
        self.PG_PORT = self.get_env("PG_PORT").unwrap()

        logger.success("Settings are OK!")
        return Ok(None)

    def convert_to_dataclass_from_dict[T](
        self: Self,
        data_class: type[T],
        data: dict[str, Any],
    ) -> Result[T, Exception]:
        """Convert dict to dataclass."""
        try:
            return Ok(
                from_dict(
                    data_class=data_class,
                    data=data,
                ),
            )
        except (
            WrongTypeError,
            MissingValueError,
            UnionMatchError,
            StrictUnionMatchError,
            UnexpectedDataError,
            ForwardReferenceError,
        ) as exc:
            return Err(exc)

    def get_telegram_url(self: Self) -> Result[str, Exception]:
        """Get url for send telegram msg."""
        return Ok(
            f"https://api.telegram.org/bot{self.TELEGRAM_BOT_API_KEY}/sendMessage",
        )

    def get_telegram_msg(
        self: Self,
        chat_id: str,
        data: str,
    ) -> Result[dict[str, bool | str], Exception]:
        """Get msg for telegram in dict."""
        return Ok(
            {
                "chat_id": chat_id,
                "parse_mode": "HTML",
                "disable_notification": True,
                "text": data,
            },
        )

    def get_chat_ids_for_telegram(self: Self) -> Result[list[str], Exception]:
        """Get list chat id for current send."""
        return Ok(self.TELEGRAM_BOT_CHAT_ID)

    def check_telegram_response(
        self: Self,
        data: TelegramSendMsg.Res,
    ) -> Result[None, Exception]:
        """Check telegram response on msg."""
        if data.ok:
            return Ok(None)
        return Err(Exception(f"{data}"))

    async def send_msg_to_each_chat_id(
        self: Self,
        chat_ids: list[str],
        data: str,
    ) -> Result[TelegramSendMsg.Res, Exception]:
        """Send msg for each chat id."""
        method = "POST"
        for chat in chat_ids:
            await do_async(
                Ok(result)
                for telegram_url in self.get_telegram_url()
                for msg in self.get_telegram_msg(chat, data)
                for msg_bytes in self.dumps_dict_to_bytes(msg)
                for response_bytes in await self.request(
                    url=telegram_url,
                    method=method,
                    headers={
                        "Content-Type": "application/json",
                    },
                    data=msg_bytes,
                )
                for response_dict in self.parse_bytes_to_dict(response_bytes)
                for data_dataclass in self.convert_to_dataclass_from_dict(
                    TelegramSendMsg.Res,
                    response_dict,
                )
                for result in self.check_telegram_response(data_dataclass)
            )
        return Ok(TelegramSendMsg.Res(ok=False))

    async def send_telegram_msg(self: Self, data: str) -> Result[None, Exception]:
        """Send msg to telegram."""
        match await do_async(
            Ok(None)
            for chat_ids in self.get_chat_ids_for_telegram()
            for _ in await self.send_msg_to_each_chat_id(chat_ids, data)
        ):
            case Err(exc):
                logger.exception(exc)
        return Ok(None)

    def get_env(self: Self, key: str) -> Result[str, ValueError]:
        """Just get key from EVN."""
        try:
            return Ok(environ[key])
        except ValueError as exc:
            logger.exception(exc)
            return Err(exc)

    def _env_convert_to_list(self: Self, data: str) -> Result[list[str], Exception]:
        """Split str by ',' character."""
        return Ok(data.split(","))

    def get_list_env(self: Self, key: str) -> Result[list[str], Exception]:
        """Get value from ENV in list[str] format.

        in .env
        KEYS=1,2,3,4,5,6

        to
        KEYS = ['1','2','3','4','5','6']
        """
        return do(
            Ok(value_in_list)
            for value_by_key in self.get_env(key)
            for value_in_list in self._env_convert_to_list(value_by_key)
        )

    async def delete_api_v1_stop_order_order_id(
        self: Self,
        order_id: str,
    ) -> Result[ApiV1StopOrderOrderIdDELETE.Res, Exception]:
        """Cancel Stop Order By OrderId.

        https://www.kucoin.com/docs-new/rest/spot-trading/orders/cancel-stop-order-by-orderld
        """
        uri = f"/api/v1/stop-order/{order_id}"
        method = "DELETE"
        return await do_async(
            Ok(result)
            for full_url in self.get_full_url(self.BASE_URL, uri)
            for now_time in self.get_now_time()
            for data_to_sign in self.cancatinate_str(now_time, method, uri)
            for headers in self.get_headers_auth(
                data_to_sign,
                now_time,
            )
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for _ in self.logger_info(response_dict)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV1StopOrderOrderIdDELETE.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    async def get_api_v1_stop_order(
        self: Self,
        params: dict[str, str],
    ) -> Result[ApiV1StopOrderGET.Res, Exception]:
        """Get Stop Orders List.

        https://www.kucoin.com/docs-new/rest/spot-trading/orders/get-stop-orders-list
        """
        uri = "/api/v1/stop-order"
        method = "GET"
        return await do_async(
            Ok(result)
            for params_in_url in self.get_url_params_as_str(params)
            for uri_params in self.cancatinate_str(uri, params_in_url)
            for full_url in self.get_full_url(self.BASE_URL, uri_params)
            for now_time in self.get_now_time()
            for data_to_sign in self.cancatinate_str(now_time, method, uri_params)
            for headers in self.get_headers_auth(
                data_to_sign,
                now_time,
            )
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV1StopOrderGET.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    async def post_api_v1_stop_order(
        self: Self,
        params: dict[str, str],
    ) -> Result[ApiV1StopOrderPOST.Res, Exception]:
        """Add Stop Order.

        https://www.kucoin.com/docs-new/rest/spot-trading/orders/add-stop-order
        """
        uri = "/api/v1/stop-order"
        method = "POST"
        logger.warning(params)
        return await do_async(
            Ok(result)
            for _ in self.logger_info(f"Margin stop order:{params}")
            for full_url in self.get_full_url(self.BASE_URL, uri)
            for dumps_data_bytes in self.dumps_dict_to_bytes(params)
            for dumps_data_str in self.decode(dumps_data_bytes)
            for now_time in self.get_now_time()
            for data_to_sign in self.cancatinate_str(
                now_time,
                method,
                uri,
                dumps_data_str,
            )
            for headers in self.get_headers_auth(
                data_to_sign,
                now_time,
            )
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
                data=dumps_data_bytes,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for _ in self.logger_info(response_dict)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV1StopOrderPOST.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    async def get_api_v2_symbols(
        self: Self,
    ) -> Result[ApiV2SymbolsGET.Res, Exception]:
        """Get symbol list.

        weight 4

        https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-all-symbols
        """
        uri = "/api/v2/symbols"
        method = "GET"
        return await do_async(
            Ok(result)
            for headers in self.get_headers_not_auth()
            for full_url in self.get_full_url(self.BASE_URL, uri)
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV2SymbolsGET.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    async def get_api_v1_market_candle(
        self: Self,
        params: dict[str, str],
    ) -> Result[ApiV1MarketCandleGET.Res, Exception]:
        """Get Klines.

        weight 3

        https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-klines
        """
        uri = "/api/v1/market/candles"
        method = "GET"
        return await do_async(
            Ok(result)
            for headers in self.get_headers_not_auth()
            for params_in_url in self.get_url_params_as_str(params)
            for uri_params in self.cancatinate_str(uri, params_in_url)
            for full_url in self.get_full_url(self.BASE_URL, uri_params)
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV1MarketCandleGET.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    async def get_api_v3_margin_accounts(
        self: Self,
        params: dict[str, str],
    ) -> Result[ApiV3MarginAccountsGET.Res, Exception]:
        """Get margin account user data.

        weight 15

        https://www.kucoin.com/docs-new/rest/account-info/account-funding/get-account-cross-margin
        """
        uri = "/api/v3/margin/accounts"
        method = "GET"
        return await do_async(
            Ok(result)
            for params_in_url in self.get_url_params_as_str(params)
            for uri_params in self.cancatinate_str(uri, params_in_url)
            for full_url in self.get_full_url(self.BASE_URL, uri_params)
            for now_time in self.get_now_time()
            for data_to_sign in self.cancatinate_str(now_time, method, uri_params)
            for headers in self.get_headers_auth(
                data_to_sign,
                now_time,
            )
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV3MarginAccountsGET.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    async def get_api_v1_accounts(
        self: Self,
        params: dict[str, str],
    ) -> Result[ApiV1AccountsGET.Res, Exception]:
        """Get Account List - Spot.

        weight 5

        https://www.kucoin.com/docs-new/rest/account-info/account-funding/get-account-list-spot
        """
        uri = "/api/v1/accounts"
        method = "GET"
        return await do_async(
            Ok(result)
            for params_in_url in self.get_url_params_as_str(params)
            for uri_params in self.cancatinate_str(uri, params_in_url)
            for full_url in self.get_full_url(self.BASE_URL, uri_params)
            for now_time in self.get_now_time()
            for data_to_sign in self.cancatinate_str(now_time, method, uri_params)
            for headers in self.get_headers_auth(
                data_to_sign,
                now_time,
            )
            for response_bytes in await self.request(
                url=full_url,
                method=method,
                headers=headers,
            )
            for response_dict in self.parse_bytes_to_dict(response_bytes)
            for data_dataclass in self.convert_to_dataclass_from_dict(
                ApiV1AccountsGET.Res,
                response_dict,
            )
            for result in self.check_response_code(data_dataclass)
        )

    def get_url_params_as_str(
        self: Self,
        params: dict[str, str],
    ) -> Result[str, Exception]:
        """Get url params in str.

        if params is empty -> ''
        if params not empty -> ?foo=bar&zoo=net
        """
        params_in_url = "&".join([f"{key}={params[key]}" for key in sorted(params)])
        if len(params_in_url) == 0:
            return Ok("")
        return Ok("?" + params_in_url)

    def get_full_url(
        self: Self,
        base_url: str,
        next_url: str,
    ) -> Result[str, Exception]:
        """Right cancatinate base url and method url."""
        return Ok(urljoin(base_url, next_url))

    def get_headers_auth(
        self: Self,
        data: str,
        now_time: str,
    ) -> Result[dict[str, str], Exception]:
        """Get headers with encrypted data for http request."""
        return do(
            Ok(
                {
                    "KC-API-SIGN": kc_api_sign,
                    "KC-API-TIMESTAMP": now_time,
                    "KC-API-PASSPHRASE": kc_api_passphrase,
                    "KC-API-KEY": self.KEY,
                    "Content-Type": "application/json",
                    "KC-API-KEY-VERSION": "2",
                    "User-Agent": "kucoin-python-sdk/2",
                },
            )
            for secret in self.encode(self.SECRET)
            for passphrase in self.encode(self.PASSPHRASE)
            for data_in_bytes in self.encode(data)
            for kc_api_sign in self.encrypt_data(secret, data_in_bytes)
            for kc_api_passphrase in self.encrypt_data(secret, passphrase)
        )

    def get_headers_not_auth(self: Self) -> Result[dict[str, str], Exception]:
        """Get headers without encripted data for http request."""
        return Ok({"User-Agent": "kucoin-python-sdk/2"})

    def convert_to_int(self: Self, data: float) -> Result[int, Exception]:
        """Convert data to int."""
        try:
            return Ok(int(data))
        except ValueError as exc:
            logger.exception(exc)
            return Err(exc)

    def get_time(self: Self) -> Result[float, Exception]:
        """Get now time as float."""
        return Ok(time())

    def get_now_time(self: Self) -> Result[str, Exception]:
        """Get now time for encrypted data."""
        return do(
            Ok(f"{time_now_in_int * 1000}")
            for time_now in self.get_time()
            for time_now_in_int in self.convert_to_int(time_now)
        )

    def check_response_code[T](
        self: Self,
        data: T,
    ) -> Result[T, Exception]:
        """Check if key `code`.

        If key `code` in dict == '200000' then success
        """
        if hasattr(data, "code") and data.code == "200000":
            return Ok(data)
        return Err(Exception(data))

    async def request(
        self: Self,
        url: str,
        method: str,
        headers: dict[str, str],
        data: bytes | None = None,
    ) -> Result[bytes, Exception]:
        """Base http request."""
        try:
            async with (
                ClientSession(
                    headers=headers,
                ) as session,
                session.request(
                    method,
                    url,
                    data=data,
                ) as response,
            ):
                res = await response.read()  # bytes
                logger.success(f"{response.status}:{method}:{url}")
                return Ok(res)
        except (ClientConnectorError, ServerDisconnectedError) as exc:
            logger.exception(exc)
            return Err(exc)

    def logger_info[T](self: Self, data: T) -> Result[T, Exception]:
        """Info logger for Pipes."""
        logger.info(data)
        return Ok(data)

    def logger_success[T](self: Self, data: T) -> Result[T, Exception]:
        """Success logger for Pipes."""
        logger.success(data)
        return Ok(data)

    def cancatinate_str(self: Self, *args: str) -> Result[str, Exception]:
        """Cancatinate to str."""
        try:
            return Ok("".join(args))
        except TypeError as exc:
            logger.exception(exc)
            return Err(exc)

    def get_default_uuid4(self: Self) -> Result[UUID, Exception]:
        """Get default uuid4."""
        return Ok(uuid4())

    def format_to_str_uuid(self: Self, data: UUID) -> Result[str, Exception]:
        """Get str UUID4 and replace `-` symbol to spaces."""
        return do(
            Ok(result) for result in self.cancatinate_str(str(data).replace("-", ""))
        )

    def get_uuid4(self: Self) -> Result[str, Exception]:
        """Get uuid4 as str without `-` symbols.

        8e7c653b-7faf-47fe-b6d3-e87c277e138a -> 8e7c653b7faf47feb6d3e87c277e138a

        get_default_uuid4 -> format_to_str_uuid
        """
        return do(
            Ok(str_uuid)
            for default_uuid in self.get_default_uuid4()
            for str_uuid in self.format_to_str_uuid(default_uuid)
        )

    def convert_bytes_to_base64(self: Self, data: bytes) -> Result[bytes, Exception]:
        """Convert bytes to base64."""
        try:
            return Ok(b64encode(data))
        except TypeError as exc:
            logger.exception(exc)
            return Err(exc)

    def encode(self: Self, data: str) -> Result[bytes, Exception]:
        """Return Ok(bytes) from str data."""
        try:
            return Ok(data.encode())
        except AttributeError as exc:
            logger.exception(exc)
            return Err(exc)

    def decode(self: Self, data: bytes) -> Result[str, Exception]:
        """Return Ok(str) from bytes data."""
        try:
            return Ok(data.decode())
        except AttributeError as exc:
            logger.exception(exc)
            return Err(exc)

    def get_default_hmac(
        self: Self,
        secret: bytes,
        data: bytes,
    ) -> Result[HMAC, Exception]:
        """Get default HMAC."""
        return Ok(hmac_new(secret, data, sha256))

    def convert_hmac_to_digest(
        self: Self,
        hmac_object: HMAC,
    ) -> Result[bytes, Exception]:
        """Convert HMAC to digest."""
        return Ok(hmac_object.digest())

    def encrypt_data(self: Self, secret: bytes, data: bytes) -> Result[str, Exception]:
        """Encript `data` to hmac."""
        return do(
            Ok(result)
            for hmac_object in self.get_default_hmac(secret, data)
            for hmac_data in self.convert_hmac_to_digest(hmac_object)
            for base64_data in self.convert_bytes_to_base64(hmac_data)
            for result in self.decode(base64_data)
        )

    def dumps_dict_to_bytes(
        self: Self,
        data: dict[str, Any],
    ) -> Result[bytes, Exception]:
        """Dumps dict to bytes[json].

        {"qaz":"edc"} -> b'{"qaz":"wsx"}'
        """
        try:
            return Ok(dumps(data))
        except JSONEncodeError as exc:
            logger.exception(exc)
            return Err(exc)

    def parse_bytes_to_dict(
        self: Self,
        data: bytes | str,
    ) -> Result[dict[str, Any], Exception]:
        """Parse bytes[json] to dict.

        b'{"qaz":"wsx"}' -> {"qaz":"wsx"}
        """
        try:
            return Ok(loads(data))
        except JSONDecodeError as exc:
            logger.exception(exc)
            return Err(exc)

    def create_book(self: Self) -> Result[None, Exception]:
        """Build own structure.

        build inside book for tickets
        book = {
            "ADA": {
                "baseincrement": Decimal,
                "priceincrement": Decimal
            },
            "JUP": {
                "baseincrement": Decimal,
                "priceincrement": Decimal
            }
        }
        """
        self.book: dict[str, Book] = {
            ticket: Book(
                baseincrement=Decimal("0"),
                priceincrement=Decimal("0"),
            )
            for ticket in self.ALL_CURRENCY
            if isinstance(ticket, str)
        }
        return Ok(None)

    def data_to_decimal(self: Self, data: float | str) -> Result[Decimal, Exception]:
        """Convert to Decimal format."""
        try:
            return Ok(Decimal(data))
        except (TypeError, InvalidOperation) as exc:
            return Err(exc)

    def export_account_usdt_from_api_v3_margin_accounts(
        self: Self,
        data: ApiV3MarginAccountsGET.Res,
    ) -> Result[ApiV3MarginAccountsGET.Res.Data.Account, Exception]:
        """Get USDT available from margin account."""
        try:
            for i in [i for i in data.data.accounts if i.currency == "USDT"]:
                return Ok(i)
            return Err(Exception("Not found USDT in accounts data"))
        except (AttributeError, KeyError) as exc:
            logger.exception(exc)
            return Err(exc)

    def compile_telegram_msg_alertest(
        self: Self,
        finance: ApiV3MarginAccountsGET.Res.Data.Account,
        debt_ratio: str,
        tokens: AlertestToken,
    ) -> Result[str, Exception]:
        """."""
        return Ok(
            f"""<b>KuCoin</b>
<i>KEEP</i>:{self.BASE_KEEP}
<i>USDT</i>:{finance.available}
<i>BORROWING USDT</i>:{finance.liability}   ({debt_ratio}%))
<i>ALL TOKENS</i>:{len(tokens.all_tokens)}
<i>USED TOKENS</i>({len(self.ALL_CURRENCY)})
<i>DELETED</i>({len(tokens.deleted_tokens)}):{",".join(tokens.deleted_tokens)}
<i>NEW</i>({len(tokens.new_tokens)}):{",".join(tokens.new_tokens)}
<i>IGNORE</i>({len(self.IGNORECURRENCY)}):{",".join(self.IGNORECURRENCY)}""",
        )

    def parse_tokens_for_alertest(
        self: Self,
        data: ApiV2SymbolsGET.Res,
    ) -> Result[AlertestToken, Exception]:
        """."""
        all_tokens: list[str] = [
            exchange_token.baseCurrency
            for exchange_token in data.data
            if exchange_token.baseCurrency not in self.IGNORECURRENCY
            and exchange_token.isMarginEnabled
            and exchange_token.quoteCurrency == "USDT"
        ]

        deleted_tokens: list[str] = [
            own_token
            for own_token in self.ALL_CURRENCY
            if (
                len(
                    [
                        exchange_token
                        for exchange_token in data.data
                        if exchange_token.baseCurrency == own_token
                        and exchange_token.isMarginEnabled
                        and exchange_token.quoteCurrency == "USDT"
                    ],
                )
                == 0
            )
        ]

        new_tokens: list[str] = [
            exchange_token.baseCurrency
            for exchange_token in data.data
            if exchange_token.baseCurrency
            not in self.ALL_CURRENCY + self.IGNORECURRENCY
            and exchange_token.isMarginEnabled
            and exchange_token.quoteCurrency == "USDT"
        ]

        return Ok(
            AlertestToken(
                all_tokens=all_tokens,
                deleted_tokens=deleted_tokens,
                new_tokens=new_tokens,
            ),
        )

    def export_debt_ratio(
        self: Self,
        data: ApiV3MarginAccountsGET.Res,
    ) -> Result[str, Exception]:
        """."""
        return Ok(data.data.debtRatio)

    async def alertest(self: Self) -> Result[None, Exception]:
        """Alert statistic."""
        logger.info("alertest")
        while True:
            await do_async(
                Ok(None)
                for api_v3_margin_accounts in await self.get_api_v3_margin_accounts(
                    params={
                        "quoteCurrency": "USDT",
                    },
                )
                for account_data in self.export_account_usdt_from_api_v3_margin_accounts(
                    api_v3_margin_accounts,
                )
                for debt_ratio in self.export_debt_ratio(api_v3_margin_accounts)
                for ticket_info in await self.get_api_v2_symbols()
                for parsed_ticked_info in self.parse_tokens_for_alertest(ticket_info)
                for tlg_msg in self.compile_telegram_msg_alertest(
                    account_data,
                    debt_ratio,
                    parsed_ticked_info,
                )
                for _ in await self.send_telegram_msg(tlg_msg)
            )
            await asyncio.sleep(60 * 60)
        return Ok(None)

    def fill_one_symbol_base_increment(
        self: Self,
        symbol: str,
        base_increment: Decimal,
    ) -> Result[None, Exception]:
        """."""
        try:
            self.book[symbol].baseincrement = base_increment
        except IndexError as exc:
            return Err(exc)
        return Ok(None)

    # nu cho jopki kak dila
    def fill_all_base_increment(
        self: Self,
        data: list[ApiV2SymbolsGET.Res.Data],
    ) -> Result[None, Exception]:
        """Fill base increment by each token."""
        for ticket in data:
            match do(
                Ok(None)
                for base_increment_decimal in self.data_to_decimal(ticket.baseIncrement)
                for _ in self.fill_one_symbol_base_increment(
                    ticket.baseCurrency,
                    base_increment_decimal,
                )
            ):
                case Err(exc):
                    return Err(exc)

        return Ok(None)

    def fill_one_symbol_price_increment(
        self: Self,
        symbol: str,
        price_increment: Decimal,
    ) -> Result[None, Exception]:
        """."""
        try:
            self.book[symbol].priceincrement = price_increment
        except IndexError as exc:
            return Err(exc)
        return Ok(None)

    def fill_all_price_increment(
        self: Self,
        data: list[ApiV2SymbolsGET.Res.Data],
    ) -> Result[None, Exception]:
        """Fill price increment by each token."""
        for ticket in data:
            match do(
                Ok(None)
                for price_increment_decimal in self.data_to_decimal(
                    ticket.priceIncrement
                )
                for _ in self.fill_one_symbol_price_increment(
                    ticket.baseCurrency,
                    price_increment_decimal,
                )
            ):
                case Err(exc):
                    return Err(exc)
        return Ok(None)

    def filter_ticket_by_book_increment(
        self: Self,
        data: ApiV2SymbolsGET.Res,
    ) -> Result[list[ApiV2SymbolsGET.Res.Data], Exception]:
        """."""
        return Ok(
            [
                out_side_ticket
                for out_side_ticket in data.data
                if out_side_ticket.baseCurrency in self.book
                and out_side_ticket.quoteCurrency == "USDT"
            ]
        )

    async def fill_increment(self: Self) -> Result[None, Exception]:
        """Fill increment from api."""
        return await do_async(
            Ok(None)
            for ticket_info in await self.get_api_v2_symbols()
            for ticket_for_fill in self.filter_ticket_by_book_increment(ticket_info)
            for _ in self.fill_all_base_increment(ticket_for_fill)
            for _ in self.fill_all_price_increment(ticket_for_fill)
        )

    def divide(
        self: Self,
        divider: Decimal,
        divisor: Decimal,
    ) -> Result[Decimal, Exception]:
        """Devide."""
        if divisor == Decimal("0"):
            return Err(ZeroDivisionError("Divisor cannot be zero"))
        return Ok(divider / divisor)

    def quantize_down(
        self: Self,
        data: Decimal,
        increment: Decimal,
    ) -> Result[Decimal, Exception]:
        """Quantize to up."""
        return Ok(data.quantize(increment, ROUND_DOWN))

    async def insert_data_to_db(
        self: Self,
        data: OrderChangeV2.Res.Data,
    ) -> Result[None, Exception]:
        """Insert data to db."""
        try:
            async with self.pool.acquire() as conn, conn.transaction():
                # Run the query passing the request argument.
                await conn.execute(
                    """INSERT INTO main(exchange, symbol, side, size, price, date) VALUES($1, $2, $3, $4, $5, $6)""",
                    "kucoin",
                    data.symbol,
                    data.side,
                    data.matchSize or "",
                    data.matchPrice,
                    datetime.now(),
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(exc)
        return Ok(None)

    async def create_db_pool(self: Self) -> Result[None, Exception]:
        """Create Postgresql connection pool."""
        try:
            self.pool: Pool[Record] = await create_pool(
                user=self.PG_USER,
                password=self.PG_PASSWORD,
                database=self.PG_DATABASE,
                host=self.PG_HOST,
                port=self.PG_PORT,
                timeout=5,
            )
            return Ok(None)
        except ConnectionRefusedError as exc:
            return Err(exc)

    async def massive_delete_api_v1_stop_order_order_id(
        self: Self,
        data: ApiV1StopOrderGET.Res,
    ) -> Result[None, Exception]:
        """."""
        for item in data.data.items:
            match await do_async(
                Ok(_)
                for _ in await self.delete_api_v1_stop_order_order_id(
                    order_id=item.id,
                )
            ):
                case Err(exc):
                    logger.exception(exc)
        return Ok(None)

    async def pre_init(self: Self) -> Result[Self, Exception]:
        """Pre-init."""
        return await do_async(
            Ok(self)
            for _ in self.init_envs()
            for _ in await self.create_db_pool()
            for _ in self.create_book()
            for _ in await self.fill_increment()
            for _ in self.logger_success(self.book)
        )

    async def get_last_hour_price_by_symbol(
        self: Self,
        symbol: str,
        cut: int,
    ) -> Result[list[list[str]], Exception]:
        """."""
        match await self.get_api_v1_market_candle(
            params={
                "symbol": symbol,
                "type": "1hour",
                "startAt": "1",
                "endAt": "0",
            }
        ):
            case Ok(candles):
                return Ok(candles.data[:cut])
            case Err(exc):
                logger.exception(exc)
                return Err(exc)
        return Err(Exception(""))

    def extract_close_price(
        self: Self, data: list[list[str]]
    ) -> Result[list[str], Exception]:
        """."""
        return Ok([close[2] for close in data])

    def calc_ma(self: Self, data: list[str]) -> Result[Decimal, Exception]:
        """."""
        result = Decimal("0")

        for close in data:
            result += Decimal(close)

        return Ok(result / len(data))

    def get_available_tokens(
        self: Self,
        ticket: str,
        data: ApiV3MarginAccountsGET.Res,
    ) -> Result[ApiV3MarginAccountsGET.Res.Data.Account, Exception]:
        """."""
        for avail in data.data.accounts:
            if avail.currency == ticket:
                return Ok(avail)
        return Err(Exception(""))

    def logic_(
        self: Self,
        ticket: str,
        ma: Decimal,
        avail_tokens: ApiV3MarginAccountsGET.Res.Data.Account,
    ) -> Result[dict[str, str], Exception]:
        """."""
        match do(
            Ok(ma_) for ma_ in self.quantize_down(ma, self.book[ticket].priceincrement)
        ):
            case Ok(ma_):
                match do(
                    Ok(size_)
                    for size_ in self.quantize_down(
                        Decimal(avail_tokens.available),
                        self.book[ticket].baseincrement,
                    )
                ):
                    case Ok(size_):
                        if size_ == 0:
                            return do(
                                Ok(
                                    {
                                        "type": "market",
                                        "symbol": f"{ticket}-USDT",
                                        "side": "buy",
                                        "funds": str(self.BASE_KEEP),
                                        "stopPrice": str(ma_),
                                        "clientOid": client_O_id,
                                        "stop": "entry",
                                    }
                                )
                                for client_O_id in self.get_uuid4()
                            )
                        if size_ > 0:
                            return do(
                                Ok(
                                    {
                                        "type": "market",
                                        "symbol": f"{ticket}-USDT",
                                        "side": "sell",
                                        "size": str(size_),
                                        "stopPrice": str(ma_),
                                        "clientOid": client_O_id,
                                        "stop": "loss",
                                    }
                                )
                                for client_O_id in self.get_uuid4()
                            )

                        return Err(Exception(""))
                    case Err(exc):
                        logger.exception(exc)
                        return Err(exc)
            case Err(exc):
                logger.exception(exc)
                return Err(exc)
        return Err(Exception(""))

    async def gg(self: Self) -> Result[str, Exception]:
        """."""
        match await do_async(
            Ok(_)
            for g in await self.get_api_v1_accounts(
                {
                    "currency": "BTC",
                    "type": "trade",
                }
            )
            for _ in self.logger_success(g)
            for g in await self.get_api_v1_accounts(
                {
                    "currency": "ETH",
                    "type": "trade",
                }
            )
            for _ in self.logger_success(g)
            for g in await self.get_api_v1_accounts(
                {
                    "currency": "SOL",
                    "type": "trade",
                }
            )
            for _ in self.logger_success(g)
        ):
            case Err(exc):
                logger.exception(exc)
        while True:
            now = datetime.now()
            target_time = now.replace(minute=1, second=0, microsecond=0) + timedelta(
                hours=1
            )
            delay = (target_time - now).total_seconds()
            await asyncio.sleep(delay)
            for ticket in self.book:
                match await do_async(
                    Ok(_)
                    for orders in await self.get_api_v1_stop_order(
                        params={"symbol": ticket + "-USDT"}
                    )
                    for _ in await self.massive_delete_api_v1_stop_order_order_id(
                        orders
                    )
                    for candles in await self.get_last_hour_price_by_symbol(
                        ticket + "-USDT",
                        200,
                    )
                    for close_prices in self.extract_close_price(candles)
                    for ma in self.calc_ma(close_prices)
                    for api_v3_margin_accounts in await self.get_api_v3_margin_accounts(
                        params={
                            "quoteCurrency": "USDT",
                        },
                    )
                    for avail_tokens in self.get_available_tokens(
                        ticket,
                        api_v3_margin_accounts,
                    )
                    for order_params in self.logic_(
                        ticket,
                        ma,
                        avail_tokens,
                    )
                    for _ in await self.post_api_v1_stop_order(order_params)
                ):
                    case Err(exc):
                        logger.exception(exc)

    async def infinity_task(self: Self) -> Result[None, Exception]:
        """Infinity run tasks."""
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(self.gg()),
            ]

        for task in tasks:
            task.result()

        return Ok(None)


# meow anton - baka des ^^


async def main() -> Result[None, Exception]:
    """Collect of major func."""
    kcn = KCN()
    match await do_async(
        Ok(None)
        for _ in await kcn.pre_init()
        for _ in kcn.logger_success("Pre-init OK!")
        for _ in await kcn.send_telegram_msg("KuCoin settings are OK!")
        for _ in await kcn.infinity_task()
    ):
        case Ok(None):
            pass
        case Err(exc):
            logger.exception(exc)
            return Err(exc)
    return Ok(None)


if __name__ == "__main__":
    """Main enter."""
    asyncio.run(main())
