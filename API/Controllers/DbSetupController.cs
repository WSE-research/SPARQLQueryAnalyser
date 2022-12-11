using Microsoft.AspNetCore.Mvc;

namespace API.Controllers;

[ApiController]
[Route("db")]
public class DbSetupController : Controller
{
    [HttpGet("types")]
    public List<Dictionary<string, dynamic>> Index()
    {
        var enums = Enum.GetValues<DbType>().Select(enumValue => new Dictionary<string, dynamic>
        {
            ["key"] = enumValue.ToString(), ["api-value"] = (int)enumValue
        }).ToList();

        return enums;
    }
}