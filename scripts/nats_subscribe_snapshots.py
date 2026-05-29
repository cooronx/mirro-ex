#!/usr/bin/env python3

import argparse
import asyncio
from typing import Iterable

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from nats.aio.client import Client as NATS


def build_proto_types():
    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = "marketdata.proto"
    file_proto.package = "mirro.marketdata"
    file_proto.syntax = "proto3"

    price_level = file_proto.message_type.add()
    price_level.name = "PriceLevel"

    field = price_level.field.add()
    field.name = "price"
    field.number = 1
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT64

    field = price_level.field.add()
    field.name = "quantity"
    field.number = 2
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT64

    snapshot = file_proto.message_type.add()
    snapshot.name = "OrderBookSnapshot"

    field = snapshot.field.add()
    field.name = "event_ts_ms"
    field.number = 1
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT64

    field = snapshot.field.add()
    field.name = "code"
    field.number = 2
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

    field = snapshot.field.add()
    field.name = "bids"
    field.number = 3
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
    field.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
    field.type_name = ".mirro.marketdata.PriceLevel"

    field = snapshot.field.add()
    field.name = "asks"
    field.number = 4
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
    field.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
    field.type_name = ".mirro.marketdata.PriceLevel"

    envelope = file_proto.message_type.add()
    envelope.name = "Envelope"

    field = envelope.field.add()
    field.name = "sequence"
    field.number = 1
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field.type = descriptor_pb2.FieldDescriptorProto.TYPE_UINT64

    field = envelope.field.add()
    field.name = "publish_ts_ms"
    field.number = 2
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT64

    field = envelope.field.add()
    field.name = "snapshot"
    field.number = 3
    field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    field.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
    field.type_name = ".mirro.marketdata.OrderBookSnapshot"

    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_proto)
    envelope_descriptor = pool.FindMessageTypeByName("mirro.marketdata.Envelope")
    envelope_type = message_factory.GetMessageClass(envelope_descriptor)
    return envelope_type


Envelope = build_proto_types()


def format_levels(levels: Iterable) -> str:
    return " ".join(f"{level.price / 10000:.4f}:{level.quantity}" for level in levels) or "-"


async def main():
    parser = argparse.ArgumentParser(
        description="Subscribe to Mirro snapshot messages from NATS and print them."
    )
    parser.add_argument(
        "--url",
        default="nats://127.0.0.1:4222",
        help="NATS server URL, default: %(default)s",
    )
    parser.add_argument(
        "--subject",
        default="market.snapshot",
        help="NATS subject to subscribe, default: %(default)s",
    )
    args = parser.parse_args()

    nc = NATS()
    await nc.connect(args.url)

    async def handle_message(msg):
        envelope = Envelope()
        envelope.ParseFromString(msg.data)
        snapshot = envelope.snapshot
        print(
            f"subject={msg.subject} sequence={envelope.sequence} "
            f"publish_ts_ms={envelope.publish_ts_ms} event_ts_ms={snapshot.event_ts_ms} "
            f"code={snapshot.code} bids=[{format_levels(snapshot.bids)}] "
            f"asks=[{format_levels(snapshot.asks)}]"
        )

    await nc.subscribe(args.subject, cb=handle_message)
    print(f"Subscribed to {args.subject} on {args.url}")

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
