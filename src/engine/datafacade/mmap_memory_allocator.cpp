#include "engine/datafacade/mmap_memory_allocator.hpp"

#include "storage/block.hpp"
#include "storage/io.hpp"
#include "storage/serialization.hpp"
#include "storage/storage.hpp"

#include "util/log.hpp"
#include "util/mmap_file.hpp"

#include "boost/assert.hpp"

namespace osrm
{
namespace engine
{
namespace datafacade
{

void readBlocks(const boost::filesystem::path &path, storage::DataLayout &layout)
{
    storage::tar::FileReader reader(path, storage::tar::FileReader::VerifyFingerprint);

    std::vector<storage::tar::FileReader::FileEntry> entries;
    reader.List(std::back_inserter(entries));

    for (const auto &entry : entries)
    {
        const auto name_end = entry.name.rfind(".meta");
        if (name_end == std::string::npos)
        {
            auto number_of_elements = reader.ReadElementCount64(entry.name);
            layout.SetBlock(entry.name,
                            storage::Block{number_of_elements, entry.size, entry.offset});
        }
    }
}

MMapMemoryAllocator::MMapMemoryAllocator(const storage::StorageConfig &config,
                                         const boost::filesystem::path &memory_file)
{
    (void)memory_file; // TODO remove
    storage::Storage storage(config);
    std::vector<std::pair<bool, boost::filesystem::path>> files = storage.GetStaticFiles();
    std::vector<std::pair<bool, boost::filesystem::path>> updatable_files =
        storage.GetUpdatableFiles();
    files.insert(files.end(), updatable_files.begin(), updatable_files.end());

    std::vector<storage::SharedDataIndex::AllocatedRegion> allocated_regions;

    constexpr bool REQUIRED = true;

    for (const auto &file : files)
    {
        if (boost::filesystem::exists(file.second))
        {
            storage::TarDataLayout layout;
            boost::iostreams::mapped_file mapped_memory_file;
            util::mmapFile<char>(file.second, mapped_memory_file);
            mapped_memory_files.push_back(std::move(mapped_memory_file));
            readBlocks(file.second, layout);
            allocated_regions.push_back({mapped_memory_file.data(), std::move(layout)});
        }
        else
        {
            if (file.first == REQUIRED)
            {
                throw util::exception("Could not find required filed: " +
                                      std::get<1>(file).string());
            }
        }
    }

    {
        // Figure out the path to the rtree file (it's not a tar file)
        auto absolute_file_index_path =
            boost::filesystem::absolute(config.GetPath(".osrm.fileIndex"));

        // Convert the boost::filesystem::path object into a plain string
        // that's stored as a member of this allocator object
        rtree_filename = absolute_file_index_path.string();

        storage::TarDataLayout fake_layout;

        fake_layout.SetBlock("/common/rtree/file_index_path",
                             {rtree_filename.size(), rtree_filename.size() + 1, 0});
        allocated_regions.push_back({&(rtree_filename[0]), std::move(fake_layout)});
    }

    index = storage::SharedDataIndex{std::move(allocated_regions)};
} // namespace datafacade

MMapMemoryAllocator::~MMapMemoryAllocator() {}

const storage::SharedDataIndex &MMapMemoryAllocator::GetIndex() { return index; }

} // namespace datafacade
} // namespace engine
} // namespace osrm
