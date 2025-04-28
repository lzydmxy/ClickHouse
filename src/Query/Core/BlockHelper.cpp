#include <Query/Core/BlockHelper.h>

namespace DB
{

NameSet BlockHelper::getNameSet(const Block & block)
{
    NameSet res;
    res.reserve(block.columns());

    for (const auto & elem : block.getNames())
        res.insert(elem);

    return res;
}

NameToType BlockHelper::getNamesToTypes(const Block & block)
{
    NameToType res;

    for (const auto & elem : block.getColumnsWithTypeAndName())
        res.emplace(elem.name, elem.type);

    return res;
}

}
