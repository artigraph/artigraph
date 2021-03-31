from __future__ import annotations

from collections.abc import Callable
from typing import Iterable, Optional, TypeVar

from mypy.nodes import ARG_POS, Argument, Context, FuncDef, TypeInfo, Var
from mypy.plugin import ClassDefContext, Plugin
from mypy.plugins.common import add_method_to_class
from mypy.types import AnyType, CallableType, Instance, NoneType, TupleType, Type

PRODUCER_FULLNAME = "arti.producers.core.Producer"

POSITIONAL_ONLY_PREFIX = "__"


class ValidationError(ValueError):
    pass


T = TypeVar("T")


def first(iter: Iterable[T], default: Optional[T] = None) -> Optional[T]:
    for item in iter:
        if item is not None:
            return item
    return default


def plugin(version: str) -> type[ArtigraphPlugin]:
    return ArtigraphPlugin


class ArtigraphPlugin(Plugin):
    def get_base_class_hook(self, fullname: str) -> Optional[Callable[[ClassDefContext], None]]:
        sym = self.lookup_fully_qualified(fullname)
        if sym and isinstance(sym.node, TypeInfo):  # pragma: no branch
            # No branching may occur if the mypy cache has not been cleared
            if any(base.fullname == PRODUCER_FULLNAME for base in sym.node.mro):
                return ProducerTransformer.transform_producer_subclass
        return None


def is_artifact_subclass(type_: Type) -> bool:
    if isinstance(type_, Instance):
        return type_.type.has_base("arti.artifacts.core.Artifact")
    return False


class ProducerTransformer:
    @classmethod
    def validate_build_args(
        cls, ctx: ClassDefContext, build: FuncDef, build_context: Context
    ) -> list[Argument]:
        build_args, all_typed = build.arguments[1:], True
        for arg in build_args:
            if arg.type_annotation is None:
                all_typed = False
                ctx.api.fail(
                    f"{ctx.cls.name}.build is missing a type hint for the {arg.variable.name} argument",
                    build_context,
                )
            else:
                type_ = ctx.api.anal_type(arg.type_annotation)
                assert type_ is not None
                if not is_artifact_subclass(type_):
                    all_typed = False
                    ctx.api.fail(
                        f"{ctx.cls.name}.build {arg.variable.name} type hint must be an Artifact subclass",
                        build_context,
                    )
        if not all_typed:
            raise ValidationError()
        return build_args

    @classmethod
    def validate_build_return(
        cls, ctx: ClassDefContext, build: FuncDef, build_context: Context
    ) -> tuple[Type, list[Type]]:
        if build.type is None:  # pragma: no cover
            ctx.api.fail(
                f"{ctx.cls.name}.build must have a return type hint", build_context,
            )
            raise ValidationError()
        assert isinstance(build.type, CallableType)
        return_type = ctx.api.anal_type(build.type.ret_type)
        assert return_type is not None
        if is_artifact_subclass(return_type):
            return_items = [return_type]
        elif isinstance(return_type, TupleType):
            return_items = return_type.items
            for i, type_ in enumerate(return_items):
                if not is_artifact_subclass(type_):
                    ctx.api.fail(
                        f"{ctx.cls.name}.build must return an Artifact for return {i}, not {type_}",
                        build_context,
                    )
                    raise ValidationError()
        elif isinstance(return_type, (AnyType, NoneType)):
            ctx.api.fail(
                f"{ctx.cls.name}.build must have a return type hint", build_context,
            )
            raise ValidationError()
        else:
            ctx.api.fail(
                f"{ctx.cls.name}.build must return an Artifact, not {return_type}", build_context,
            )
            raise ValidationError()
        return return_type, return_items

    @classmethod
    def transform_producer_subclass(cls, ctx: ClassDefContext) -> None:
        # TODO: Feature parity with Producer._validate_*
        # TODO: Assert no defaults
        # TODO: Verify build arguments (not just return)
        # TODO: If map is implemented, confirm:
        #     - at least the input or output Artifacts are partitioned
        #     - output partition types match (ie: same frequency, all unpartitioned, etc)
        #     - signature corresponds to build
        build = first(info.get_method("build") for info in ctx.cls.info.mro)
        if build is None:
            ctx.api.fail(
                f"{ctx.cls.name} does not define a build method",
                Context(column=ctx.cls.column, line=ctx.cls.line),
            )
            return

        assert isinstance(build, FuncDef)
        build_context = Context(column=build.column, line=build.line)
        try:
            build_args = cls.validate_build_args(ctx, build, build_context)
            build_ret_type, build_outputs = cls.validate_build_return(ctx, build, build_context)
        except ValidationError:
            return

        add_method_to_class(
            api=ctx.api, cls=ctx.cls, name="__init__", args=build_args, return_type=NoneType(),
        )
        add_method_to_class(
            api=ctx.api,
            cls=ctx.cls,
            name="to",
            args=[
                Argument(
                    initializer=None,
                    kind=ARG_POS,
                    type_annotation=type_,
                    variable=Var(f"{POSITIONAL_ONLY_PREFIX}{i}"),
                )
                for i, type_ in enumerate(build_outputs)
            ],
            return_type=build_ret_type,
        )
