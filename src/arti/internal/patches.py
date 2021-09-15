from typing import no_type_check


@no_type_check
def patch_pydantic_ModelField__type_analysis():  # noqa # pragma: no cover
    """Patch to work around a bug causing dict subclasses to be converted to stock dicts[1].

    A PR to fix has been submitted[2] but not yet released - this patch pulls from that.

    This is required to use arti.internal.utils.frozendict as a Model attribute.

    1: https://github.com/samuelcolvin/pydantic/issues/3122
    2: https://github.com/samuelcolvin/pydantic/pull/3138
    """

    from pydantic.version import VERSION

    if tuple(int(i) for i in VERSION.split(".")) >= (1, 9):
        return

    from collections.abc import Hashable as CollectionsHashable
    from collections.abc import Iterable as CollectionsIterable
    from typing import (
        Any,
        Counter,
        DefaultDict,
        Deque,
        Dict,
        FrozenSet,
        Iterable,
        List,
        Mapping,
        Pattern,
        Sequence,
        Set,
        Tuple,
        Type,
        TypeVar,
        Union,
    )

    from pydantic.class_validators import Validator
    from pydantic.fields import (
        SHAPE_DEFAULTDICT,
        SHAPE_DEQUE,
        SHAPE_DICT,
        SHAPE_FROZENSET,
        SHAPE_GENERIC,
        SHAPE_ITERABLE,
        SHAPE_LIST,
        SHAPE_MAPPING,
        SHAPE_NAME_LOOKUP,
        SHAPE_SEQUENCE,
        SHAPE_SET,
        SHAPE_TUPLE,
        SHAPE_TUPLE_ELLIPSIS,
        ModelField,
        Undefined,
    )
    from pydantic.types import Json, JsonWrapper
    from pydantic.typing import (
        Callable,
        NoneType,
        display_as_type,
        get_args,
        get_origin,
        is_literal_type,
        is_new_type,
        is_typeddict,
        new_type_supertype,
    )
    from pydantic.utils import lenient_issubclass
    from typing_extensions import Annotated

    #################
    # SHAPE_COUNTER #
    #################

    SHAPE_COUNTER = 14
    SHAPE_NAME_LOOKUP[SHAPE_COUNTER] = "Counter[{}]"

    ###################
    # is_union_origin #
    ###################

    def is_union_origin(tp: Type[Any]) -> bool:
        return tp is Union

    ##################
    # _type_analysis #
    ##################

    def _type_analysis(self) -> None:  # noqa: C901 (ignore complexity)
        # typing interface is horrible, we have to do some ugly checks
        if lenient_issubclass(self.type_, JsonWrapper):
            self.type_ = self.type_.inner_type
            self.parse_json = True
        elif lenient_issubclass(self.type_, Json):
            self.type_ = Any
            self.parse_json = True
        elif isinstance(self.type_, TypeVar):
            if self.type_.__bound__:
                self.type_ = self.type_.__bound__
            elif self.type_.__constraints__:
                self.type_ = Union[self.type_.__constraints__]
            else:
                self.type_ = Any
        elif is_new_type(self.type_):
            self.type_ = new_type_supertype(self.type_)

        if self.type_ is Any or self.type_ is object:
            if self.required is Undefined:
                self.required = False
            self.allow_none = True
            return
        elif self.type_ is Pattern:
            # python 3.7 only, Pattern is a typing object but without sub fields
            return
        elif is_literal_type(self.type_):
            return
        elif is_typeddict(self.type_):
            return

        origin = get_origin(self.type_)
        # add extra check for `collections.abc.Hashable` for python 3.10+ where origin is not `None`
        if origin is None or origin is CollectionsHashable:
            # field is not "typing" object eg. Union, Dict, List etc.
            # allow None for virtual superclasses of NoneType, e.g. Hashable
            if isinstance(self.type_, type) and isinstance(None, self.type_):
                self.allow_none = True
            return
        elif origin is Annotated:
            self.type_ = get_args(self.type_)[0]
            self._type_analysis()
            return
        elif origin is Callable:
            return
        elif is_union_origin(origin):
            types_ = []
            for type_ in get_args(self.type_):
                if type_ is NoneType:
                    if self.required is Undefined:
                        self.required = False
                    self.allow_none = True
                    continue
                types_.append(type_)

            if len(types_) == 1:
                # Optional[]
                self.type_ = types_[0]
                # this is the one case where the "outer type" isn't just the original type
                self.outer_type_ = self.type_
                # re-run to correctly interpret the new self.type_
                self._type_analysis()
            else:
                self.sub_fields = [
                    self._create_sub_type(t, f"{self.name}_{display_as_type(t)}") for t in types_
                ]
            return
        elif issubclass(origin, Tuple):
            # origin == Tuple without item type
            args = get_args(self.type_)
            if not args:  # plain tuple
                self.type_ = Any
                self.shape = SHAPE_TUPLE_ELLIPSIS
            elif len(args) == 2 and args[1] is Ellipsis:  # e.g. Tuple[int, ...]
                self.type_ = args[0]
                self.shape = SHAPE_TUPLE_ELLIPSIS
                self.sub_fields = [self._create_sub_type(args[0], f"{self.name}_0")]
            elif args == ((),):  # Tuple[()] means empty tuple
                self.shape = SHAPE_TUPLE
                self.type_ = Any
                self.sub_fields = []
            else:
                self.shape = SHAPE_TUPLE
                self.sub_fields = [
                    self._create_sub_type(t, f"{self.name}_{i}") for i, t in enumerate(args)
                ]
            return
        elif issubclass(origin, List):
            # Create self validators
            get_validators = getattr(self.type_, "__get_validators__", None)
            if get_validators:
                self.class_validators.update(
                    {
                        f"list_{i}": Validator(validator, pre=True)
                        for i, validator in enumerate(get_validators())
                    }
                )

            self.type_ = get_args(self.type_)[0]
            self.shape = SHAPE_LIST
        elif issubclass(origin, Set):
            # Create self validators
            get_validators = getattr(self.type_, "__get_validators__", None)
            if get_validators:
                self.class_validators.update(
                    {
                        f"set_{i}": Validator(validator, pre=True)
                        for i, validator in enumerate(get_validators())
                    }
                )

            self.type_ = get_args(self.type_)[0]
            self.shape = SHAPE_SET
        elif issubclass(origin, FrozenSet):
            self.type_ = get_args(self.type_)[0]
            self.shape = SHAPE_FROZENSET
        elif issubclass(origin, Deque):
            self.type_ = get_args(self.type_)[0]
            self.shape = SHAPE_DEQUE
        elif issubclass(origin, Sequence):
            self.type_ = get_args(self.type_)[0]
            self.shape = SHAPE_SEQUENCE
        # priority to most common mapping: dict
        elif origin is dict or origin is Dict:
            self.key_field = self._create_sub_type(
                get_args(self.type_)[0], "key_" + self.name, for_keys=True
            )
            self.type_ = get_args(self.type_)[1]
            self.shape = SHAPE_DICT
        elif issubclass(origin, DefaultDict):
            self.key_field = self._create_sub_type(
                get_args(self.type_)[0], "key_" + self.name, for_keys=True
            )
            self.type_ = get_args(self.type_)[1]
            self.shape = SHAPE_DEFAULTDICT
        elif issubclass(origin, Counter):
            self.key_field = self._create_sub_type(
                get_args(self.type_)[0], "key_" + self.name, for_keys=True
            )
            self.type_ = int
            self.shape = SHAPE_COUNTER
        elif issubclass(origin, Mapping):
            self.key_field = self._create_sub_type(
                get_args(self.type_)[0], "key_" + self.name, for_keys=True
            )
            self.type_ = get_args(self.type_)[1]
            self.shape = SHAPE_MAPPING
        # Equality check as almost everything inherits form Iterable, including str
        # check for Iterable and CollectionsIterable, as it could receive one even when declared with the other
        elif origin in {Iterable, CollectionsIterable}:
            self.type_ = get_args(self.type_)[0]
            self.shape = SHAPE_ITERABLE
            self.sub_fields = [self._create_sub_type(self.type_, f"{self.name}_type")]
        elif issubclass(origin, Type):
            return
        elif hasattr(origin, "__get_validators__") or self.model_config.arbitrary_types_allowed:
            # Is a Pydantic-compatible generic that handles itself
            # or we have arbitrary_types_allowed = True
            self.shape = SHAPE_GENERIC
            self.sub_fields = [
                self._create_sub_type(t, f"{self.name}_{i}")
                for i, t in enumerate(get_args(self.type_))
            ]
            self.type_ = origin
            return
        else:
            raise TypeError(f'Fields of type "{origin}" are not supported.')

        # type_ has been refined eg. as the type of a List and sub_fields needs to be populated
        self.sub_fields = [self._create_sub_type(self.type_, "_" + self.name)]

    ModelField._type_analysis = _type_analysis
