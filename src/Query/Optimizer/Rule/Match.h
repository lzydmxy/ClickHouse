
#pragma once

#include <QueryPlan/PlanNode.h>

#include <any>
#include <atomic>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace DB
{

using Capture = std::string_view;

class Captures : public std::unordered_multimap<Capture, std::any>
{
public:
    template <typename T>
    T at(const Capture & capture) const
    {
        auto iters = equal_range(capture);
        auto next = iters.first;

        if (iters.first == iters.second || ++next != iters.second) {
            throw Exception("Not unique capture for this capture key: " + String{capture}, ErrorCodes::LOGICAL_ERROR);
        }

        return std::any_cast<T>(iters.first->second);
    }
};

class Match
{
public:
    explicit Match(const Captures & captures_): captures(captures_) {} // NOLINT
    explicit Match(Captures && captures_): captures(std::move(captures_)) {}

    Captures captures;
};

}
