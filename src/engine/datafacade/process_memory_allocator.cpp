#include "engine/datafacade/process_memory_allocator.hpp"
#include "storage/storage.hpp"

#include "boost/assert.hpp"

namespace osrm
{
namespace engine
{
namespace datafacade
{

ProcessMemoryAllocator::ProcessMemoryAllocator(const storage::StorageConfig &config)
{
    storage::Storage storage(config);

    // mmap each of these memory blocks, etc
    // pass the list of iterators to index = storage::SharedDataIndex({{internal_memory.get(),
    // std::move(layout)}}); on line 34 (for nao)

    // Calculate the layout/size of the memory block
    storage::DataLayout layout;
    storage.PopulateStaticLayout(layout);
    storage.PopulateUpdatableLayout(layout);

    // Allocate the memory block, then load data from files into it
    internal_memory = std::make_unique<char[]>(
        layout.GetSizeOfLayout()); // goes off and grabs one big chucnk of ram
    // could be
    // internal_memory = malloc(layout.GetSizeOfLayout());
    // RAII - resource allocation is initialization
    // std::unique_ptr -- gives you a way to use a pointer on the stack (otherwise it would be a
    // copy of the object itself)

    index = storage::SharedDataIndex({{internal_memory.get(), std::move(layout)}});

    storage.PopulateStaticData(index);
    storage.PopulateUpdatableData(index);
}

ProcessMemoryAllocator::~ProcessMemoryAllocator() { /* free(internal_memory) */}

const storage::SharedDataIndex &ProcessMemoryAllocator::GetIndex() { return index; }

} // namespace datafacade
} // namespace engine
} // namespace osrm
